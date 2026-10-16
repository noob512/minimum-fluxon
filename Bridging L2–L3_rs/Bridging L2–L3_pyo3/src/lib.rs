use std::collections::{BTreeMap, BTreeSet};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use Bridging L2–L3_kv::client_kv_api::ClientKvApiViewTrait;
use Bridging L2–L3_kv::cluster_manager::ClusterManagerViewTrait;
use Bridging L2–L3_kv::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use Bridging L2–L3_kv::config::{ClientConfigYaml, MasterConfigYaml};
use Bridging L2–L3_kv::p2p::msg_pack::{MsgPack, call_rpc};
use Bridging L2–L3_kv::p2p::p2p_module::{UserRpcHandler, user_rpc_register_handler};
use Bridging L2–L3_kv::rpcresp_kvresult_convert::msg_and_error::{
    ApiError as CoreApiError, KvError as CoreKvError, KvResult, OK,
};
use Bridging L2–L3_kv::user_api::FlatDict;
use Bridging L2–L3_kv::user_api::FlatValue;
use Bridging L2–L3_kv::user_api::Bridging L2–L3UserApi;
use Bridging L2–L3_kv::{
    ConfigArg, Framework, KvClientTrait, KvGetResultWithSource,
    config::{ClientConfig, MasterConfig},
    run_client, run_master,
};
use Bridging L2–L3_util::run_async_from_sync::{SyncAsyncBridge, borrow_stable_owner};
use parking_lot::Mutex;
use pyo3::exceptions::{
    PyKeyboardInterrupt, PyOSError, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyModule, PyString, PyTuple};
use serde::Deserialize;
use serde_yaml::{Mapping, Value};
use std::os::fd::IntoRawFd;
use std::time::Duration;
use tokio::runtime::Runtime;

// Unit tests build a native test binary, so PyO3's extension-module mode must
// link libpython there. The cdylib/wheel build keeps Python symbols unresolved.
#[cfg(test)]
include!(env!("Bridging L2–L3_PYO3_TEST_PYTHON_LINK_RS"));

mod memholder;
pub use memholder::{ExternalMemHolder, MemHolder};
mod flatdict_zerocopy;
mod kvfuture;
pub use kvfuture::KvFuture;
mod error;
use error::{
    new_backend_init_failed_error, new_general_error, new_invalid_argument_error,
    new_key_not_found_error, new_network_error, new_none_success_instance,
};
mod etcd;
pub use etcd::{PyEtcdKvClient, PyEtcdLock};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RdmavDriverEnvUpdate {
    previous_rdmav_drivers: Option<String>,
    previous_ibv_drivers: Option<String>,
    driver_list: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundledIbverbsDriverConfigEntry {
    config_path: PathBuf,
    driver_name: String,
    provider_path: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BundledIbverbsDriverDiscovery {
    config_dir: Option<PathBuf>,
    config_paths: Vec<PathBuf>,
    entries: Vec<BundledIbverbsDriverConfigEntry>,
    outcomes: Vec<String>,
}

fn read_non_empty_env_var(name: &str) -> Option<String> {
    let value = std::env::var(name).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn configure_bundled_rdmav_driver_env(driver_names: &[String]) -> Option<RdmavDriverEnvUpdate> {
    if driver_names.is_empty() {
        return None;
    }
    let update = RdmavDriverEnvUpdate {
        previous_rdmav_drivers: read_non_empty_env_var("RDMAV_DRIVERS"),
        previous_ibv_drivers: read_non_empty_env_var("IBV_DRIVERS"),
        driver_list: driver_names.join(":"),
    };
    unsafe {
        std::env::set_var("RDMAV_DRIVERS", &update.driver_list);
        std::env::set_var("IBV_DRIVERS", &update.driver_list);
    }
    Some(update)
}

fn bundled_provider_dirs(libs_dir: &Path) -> Vec<PathBuf> {
    let mut provider_dirs = vec![libs_dir.to_path_buf()];
    let bundled_provider_dir = libs_dir.join("libibverbs");
    if bundled_provider_dir.is_dir() {
        provider_dirs.push(bundled_provider_dir);
    }
    provider_dirs
}

fn parse_bundled_ibverbs_driver_name(driver_config_text: &str) -> Option<String> {
    for raw_line in driver_config_text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let directive = parts.next()?;
        let driver_name = parts.next()?;
        if directive != "driver" || parts.next().is_some() {
            return None;
        }
        return Some(driver_name.to_string());
    }
    None
}

fn bundled_provider_library_paths_for_driver(libs_dir: &Path, driver_name: &str) -> Vec<PathBuf> {
    let file_prefix = format!("lib{driver_name}-rdmav");
    let mut provider_paths = Vec::new();
    let mut seen_provider_paths = BTreeSet::new();
    for provider_dir in bundled_provider_dirs(libs_dir) {
        let Ok(entries) = std::fs::read_dir(&provider_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.starts_with(&file_prefix) || !file_name.contains(".so") {
                continue;
            }
            let provider_key = path.to_string_lossy().to_string();
            if !seen_provider_paths.insert(provider_key) {
                continue;
            }
            provider_paths.push(path);
        }
    }
    provider_paths.sort();
    provider_paths
}

fn bundled_driver_names_from_entries(entries: &[BundledIbverbsDriverConfigEntry]) -> Vec<String> {
    let mut driver_names = BTreeSet::new();
    for entry in entries {
        driver_names.insert(entry.driver_name.clone());
    }
    driver_names.into_iter().collect()
}

fn discover_bundled_ibverbs_driver_config(libs_dir: &Path) -> BundledIbverbsDriverDiscovery {
    let mut discovery = BundledIbverbsDriverDiscovery::default();
    let config_dir = libs_dir.join("libibverbs.d");
    if !config_dir.is_dir() {
        discovery
            .outcomes
            .push(format!("config_dir_missing:{}", config_dir.display()));
        return discovery;
    }
    discovery.config_dir = Some(config_dir.clone());

    let mut config_paths = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&config_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name.ends_with(".driver") {
                config_paths.push(path);
            }
        }
    }
    config_paths.sort();
    discovery.config_paths = config_paths.clone();
    if config_paths.is_empty() {
        discovery
            .outcomes
            .push(format!("config_dir_empty:{}", config_dir.display()));
    }

    for config_path in config_paths {
        let path_text = config_path.display().to_string();
        let driver_config_text = match std::fs::read_to_string(&config_path) {
            Ok(text) => text,
            Err(err) => {
                discovery
                    .outcomes
                    .push(format!("config_read_fail:{path_text}:{err}"));
                continue;
            }
        };
        let Some(driver_name) = parse_bundled_ibverbs_driver_name(&driver_config_text) else {
            discovery
                .outcomes
                .push(format!("config_parse_fail:{path_text}"));
            continue;
        };
        let provider_matches = bundled_provider_library_paths_for_driver(libs_dir, &driver_name);
        match provider_matches.as_slice() {
            [] => {
                discovery.outcomes.push(format!(
                    "provider_missing:{path_text}=>driver={driver_name}"
                ));
            }
            [provider_path] => {
                discovery.outcomes.push(format!(
                    "config_ok:{path_text}=>driver={driver_name}=>provider={}",
                    provider_path.display()
                ));
                discovery.entries.push(BundledIbverbsDriverConfigEntry {
                    config_path: config_path.clone(),
                    driver_name,
                    provider_path: provider_path.clone(),
                });
            }
            _ => {
                discovery.outcomes.push(format!(
                    "provider_ambiguous:{path_text}=>driver={driver_name}=>candidates={}",
                    provider_matches
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
        }
    }

    discovery
}

fn path_contains_Bridging L2–L3_pyo3_libs_dir(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "Bridging L2–L3_pyo3.libs")
}

fn sanitize_bundled_ld_library_path_entries(
    authoritative_lib_path: &Path,
    current_ld_library_path: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let authoritative_entry = authoritative_lib_path.to_string_lossy().to_string();
    let mut sanitized_entries = vec![authoritative_entry.clone()];
    let mut removed_entries = Vec::new();
    let mut seen_sanitized_entries = BTreeSet::from([authoritative_entry]);
    let mut seen_removed_entries = BTreeSet::new();

    let Some(current_ld_library_path) = current_ld_library_path else {
        return (sanitized_entries, removed_entries);
    };

    for raw_entry in current_ld_library_path.split(':') {
        let entry = raw_entry.trim();
        if entry.is_empty() {
            continue;
        }
        if entry == sanitized_entries[0] {
            continue;
        }
        if path_contains_Bridging L2–L3_pyo3_libs_dir(Path::new(entry)) {
            if seen_removed_entries.insert(entry.to_string()) {
                removed_entries.push(entry.to_string());
            }
            continue;
        }
        if seen_sanitized_entries.insert(entry.to_string()) {
            sanitized_entries.push(entry.to_string());
        }
    }

    (sanitized_entries, removed_entries)
}

fn set_authoritative_bundled_ld_library_path(
    authoritative_lib_path: &Path,
) -> (Option<String>, Vec<String>, Vec<String>) {
    let previous_ld_library_path = std::env::var("LD_LIBRARY_PATH").ok();
    let (sanitized_entries, removed_entries) = sanitize_bundled_ld_library_path_entries(
        authoritative_lib_path,
        previous_ld_library_path.as_deref(),
    );
    unsafe {
        std::env::set_var("LD_LIBRARY_PATH", sanitized_entries.join(":"));
    }
    (previous_ld_library_path, sanitized_entries, removed_entries)
}

fn extract_Bridging L2–L3_pyo3_libs_root_from_path(path: &Path) -> Option<String> {
    let mut root = PathBuf::new();
    let mut found_root = false;
    for component in path.components() {
        root.push(component.as_os_str());
        if component.as_os_str() == "Bridging L2–L3_pyo3.libs" {
            found_root = true;
            break;
        }
    }
    found_root.then(|| root.display().to_string())
}

fn extract_Bridging L2–L3_pyo3_libs_root_from_loaded_library_line(line: &str) -> Option<String> {
    line.split_whitespace()
        .find(|token| token.contains("Bridging L2–L3_pyo3.libs"))
        .and_then(|token| extract_Bridging L2–L3_pyo3_libs_root_from_path(Path::new(token)))
}

fn loaded_Bridging L2–L3_pyo3_libs_roots(relevant_loaded_libraries: &[String]) -> Vec<String> {
    let mut roots = BTreeSet::new();
    for line in relevant_loaded_libraries {
        if let Some(root) = extract_Bridging L2–L3_pyo3_libs_root_from_loaded_library_line(line) {
            roots.insert(root);
        }
    }
    roots.into_iter().collect()
}

fn validate_single_Bridging L2–L3_pyo3_libs_root(
    authoritative_root: Option<&str>,
    relevant_loaded_libraries: &[String],
) -> Result<Vec<String>, String> {
    let loaded_roots = loaded_Bridging L2–L3_pyo3_libs_roots(relevant_loaded_libraries);
    if loaded_roots.len() > 1 {
        return Err(format!(
            "multiple Bridging L2–L3_pyo3.libs roots detected; authoritative_root={:?} loaded_roots={:?}",
            authoritative_root, loaded_roots
        ));
    }
    if let (Some(authoritative_root), Some(loaded_root)) =
        (authoritative_root, loaded_roots.first())
    {
        if loaded_root != authoritative_root {
            return Err(format!(
                "loaded Bridging L2–L3_pyo3.libs root does not match authoritative root; authoritative_root={} loaded_root={}",
                authoritative_root, loaded_root
            ));
        }
    }
    Ok(loaded_roots)
}

fn read_relevant_loaded_libraries() -> Vec<String> {
    let Ok(maps) = std::fs::read_to_string("/proc/self/maps") else {
        return Vec::new();
    };
    let mut relevant_entries = BTreeSet::new();
    for line in maps.lines() {
        if line.contains("Bridging L2–L3_pyo3.libs") {
            relevant_entries.insert(line.to_string());
        }
    }
    relevant_entries.into_iter().collect()
}

fn enforce_single_Bridging L2–L3_pyo3_libs_root(
    call_site: &'static str,
    authoritative_root: Option<&Path>,
) -> Result<(), String> {
    let relevant_loaded_libraries = read_relevant_loaded_libraries();
    let authoritative_root_text = authoritative_root.map(|path| path.display().to_string());
    validate_single_Bridging L2–L3_pyo3_libs_root(
        authoritative_root_text.as_deref(),
        &relevant_loaded_libraries,
    )
    .map(|_| ())
    .map_err(|detail| {
        format!(
            "{detail}; call_site={call_site} relevant_loaded_libraries={relevant_loaded_libraries:?}"
        )
    })
}

#[pyfunction]
fn require_kv_framework(client: &KvClient) -> PyResult<Arc<Framework>> {
    if client.kv_shutdown.is_requested() {
        return Err(PyRuntimeError::new_err("KvClient is closing"));
    }
    client
        .framework
        .as_ref()
        .cloned()
        .ok_or_else(|| PyRuntimeError::new_err("KvClient is closed"))
}

fn require_kv_framework_api(client: &KvClient, py: Python) -> Result<Arc<Framework>, PyObject> {
    if client.kv_shutdown.is_requested() {
        return Err(new_general_error(py, "Client is closing"));
    }
    client
        .framework
        .as_ref()
        .cloned()
        .ok_or_else(|| new_general_error(py, "Client is closed"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KvShutdownRequest {
    Open,
    CloseRequested,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvShutdownProgress {
    Pending,
    AttemptFailed { attempt: u64, detail: String },
    Finished,
}

/// Owns the one KV shutdown flow independently from synchronous Python callers.
#[derive(Clone)]
struct KvFrameworkShutdown {
    framework: Arc<Mutex<Option<Arc<Framework>>>>,
    request_tx: tokio::sync::watch::Sender<KvShutdownRequest>,
    progress_rx: tokio::sync::watch::Receiver<KvShutdownProgress>,
}

impl KvFrameworkShutdown {
    fn new(runtime: &tokio::runtime::Handle, framework: Arc<Framework>) -> Self {
        let (request_tx, mut request_rx) = tokio::sync::watch::channel(KvShutdownRequest::Open);
        let (progress_tx, progress_rx) = tokio::sync::watch::channel(KvShutdownProgress::Pending);
        let framework_slot = Arc::new(Mutex::new(Some(framework.clone())));
        let owner_framework_slot = framework_slot.clone();

        // This task is outside the KV task registry because it drives registry shutdown.
        let _shutdown_owner = runtime.spawn(async move {
            loop {
                if *request_rx.borrow_and_update() == KvShutdownRequest::CloseRequested {
                    break;
                }
                if request_rx.changed().await.is_err() {
                    return;
                }
            }

            let mut attempt = 1_u64;
            loop {
                match framework.shutdown().await {
                    Ok(()) => {
                        drop(framework);
                        owner_framework_slot.lock().take();
                        progress_tx.send_replace(KvShutdownProgress::Finished);
                        return;
                    }
                    Err(err) => {
                        let detail = err.to_string();
                        if attempt == 1 || attempt.is_power_of_two() {
                            tracing::warn!(
                                attempt,
                                error = %detail,
                                "KV framework shutdown attempt failed; background owner will retry"
                            );
                        }
                        progress_tx
                            .send_replace(KvShutdownProgress::AttemptFailed { attempt, detail });
                        attempt = attempt
                            .checked_add(1)
                            .expect("KV shutdown attempt counter overflow");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Self {
            framework: framework_slot,
            request_tx,
            progress_rx,
        }
    }

    fn is_requested(&self) -> bool {
        *self.request_tx.borrow() == KvShutdownRequest::CloseRequested
    }

    fn request(&self) {
        self.request_tx
            .send_replace(KvShutdownRequest::CloseRequested);
        if let Some(framework) = self.framework.lock().as_ref().cloned() {
            framework.request_pre_shutdown();
        }
    }

    async fn shutdown_attempt(&self) -> Result<(), String> {
        let seen_attempt = match &*self.progress_rx.borrow() {
            KvShutdownProgress::AttemptFailed { attempt, .. } => *attempt,
            KvShutdownProgress::Pending | KvShutdownProgress::Finished => 0,
        };
        self.request();
        let mut progress_rx = self.progress_rx.clone();
        loop {
            match progress_rx.borrow().clone() {
                KvShutdownProgress::Pending => {}
                KvShutdownProgress::AttemptFailed { attempt, detail } if attempt > seen_attempt => {
                    return Err(detail);
                }
                KvShutdownProgress::AttemptFailed { .. } => {}
                KvShutdownProgress::Finished => return Ok(()),
            }
            if progress_rx.changed().await.is_err() {
                return Err("KV shutdown owner stopped before publishing completion".to_string());
            }
        }
    }

    async fn wait_until_finished(&self) -> Result<(), String> {
        self.request();
        let mut progress_rx = self.progress_rx.clone();
        loop {
            match progress_rx.borrow().clone() {
                KvShutdownProgress::Pending | KvShutdownProgress::AttemptFailed { .. } => {}
                KvShutdownProgress::Finished => return Ok(()),
            }
            if progress_rx.changed().await.is_err() {
                return Err("KV shutdown owner stopped before publishing completion".to_string());
            }
        }
    }
}

struct KvClientDropCleanup {
    framework: Option<Arc<Framework>>,
    runtime: Option<Runtime>,
    shutdown: Option<KvFrameworkShutdown>,
}

impl KvClientDropCleanup {
    fn new(framework: Arc<Framework>, runtime: Runtime, shutdown: KvFrameworkShutdown) -> Self {
        Self {
            framework: Some(framework),
            runtime: Some(runtime),
            shutdown: Some(shutdown),
        }
    }

    fn run(mut self) {
        let shutdown_result = {
            let shutdown = self
                .shutdown
                .as_ref()
                .expect("drop cleanup shutdown owner exists");
            let runtime = self.runtime.as_mut().expect("drop cleanup runtime exists");
            runtime.block_on(shutdown.wait_until_finished())
        };
        if let Err(detail) = shutdown_result {
            tracing::error!(
                "KV client background Drop cleanup failed; retaining storage/KV resources: {}",
                detail
            );
            return;
        }

        drop(self.shutdown.take());
        drop(self.framework.take());
        self.runtime
            .take()
            .expect("drop cleanup runtime exists after successful shutdown")
            .shutdown_background();
    }
}

impl Drop for KvClientDropCleanup {
    fn drop(&mut self) {
        if self.framework.is_none() && self.runtime.is_none() && self.shutdown.is_none() {
            return;
        }
        // No completion barrier means the KV mmap may still be referenced. Retain the entire
        // dependency chain instead of allowing field Drop to unmap memory out from under storage.
        tracing::error!("retaining storage/KV resources because Drop cleanup did not complete");
        if let Some(framework) = self.framework.take() {
            std::mem::forget(framework);
        }
        if let Some(runtime) = self.runtime.take() {
            std::mem::forget(runtime);
        }
        if let Some(shutdown) = self.shutdown.take() {
            std::mem::forget(shutdown);
        }
    }
}

// moved to crate::error::new_transfer_block_failed_error

// error helpers moved to crate::error

/// Result type that can be either a success value or an API error
#[derive(Debug)]
enum ApiResult<T> {
    Success(T),
    Error(PyObject),
}

impl<T> ApiResult<T> {
    fn new_success(value: T) -> Self {
        ApiResult::Success(value)
    }

    fn new_error(error: PyObject) -> Self {
        ApiResult::Error(error)
    }

    fn into_py_object(self, py: Python) -> PyObject
    where
        T: IntoPy<PyObject>,
    {
        match self {
            ApiResult::Success(value) => crate::error::new_result_success(py, value.into_py(py)),
            ApiResult::Error(error) => crate::error::new_result_error(py, error),
        }
    }
}

struct PyUserRpcHandlerRaw {
    handler: PyObject,
}

impl UserRpcHandler for PyUserRpcHandlerRaw {
    fn handle(
        &self,
        from_node: Bridging L2–L3_kv::cluster_manager::NodeID,
        payload: &[u8],
    ) -> Result<Vec<u8>, CoreKvError> {
        Python::with_gil(|py| {
            let payload_py = PyBytes::new_bound(py, payload);
            let args =
                PyTuple::new_bound(py, &[from_node.to_string().into_py(py), payload_py.into()]);
            let out = self.handler.call1(py, args).map_err(|e| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("python rpc handler raised: {}", e),
                })
            })?;

            let out_bytes = out.downcast_bound::<PyBytes>(py).map_err(|_| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: "rpc handler must return bytes".to_string(),
                })
            })?;
            let out_bytes = out_bytes.as_bytes().to_vec();
            Ok(out_bytes)
        })
    }
}

/// Initialize dynamic libraries for the module
fn init_dynamic_libraries() -> PyResult<()> {
    #[cfg(unix)]
    {
        use std::ffi::{CStr, CString};
        use std::os::raw::{c_char, c_void};

        unsafe extern "C" {
            fn dlopen(filename: *const c_char, flag: i32) -> *mut c_void;
            fn dladdr(addr: *const c_void, info: *mut DlInfo) -> i32;
        }

        const RTLD_NOW: i32 = 2;
        const RTLD_GLOBAL: i32 = 0x100;

        #[repr(C)]
        struct DlInfo {
            dli_fname: *const c_char,
            dli_fbase: *mut c_void,
            dli_sname: *const c_char,
            dli_saddr: *mut c_void,
        }

        fn preload_by_path(path: &PathBuf, flags: i32, preload_outcomes: &mut Vec<String>) -> bool {
            let path_text = path.to_string_lossy().to_string();
            match CString::new(path_text.clone()) {
                Ok(path_cstr) => unsafe {
                    let handle = dlopen(path_cstr.as_ptr(), flags);
                    if !handle.is_null() {
                        preload_outcomes.push(format!("path_ok:{path_text}"));
                        true
                    } else {
                        preload_outcomes.push(format!("path_fail:{path_text}"));
                        false
                    }
                },
                Err(_) => {
                    preload_outcomes.push(format!("path_invalid:{path_text}"));
                    false
                }
            }
        }

        fn module_libs_dir() -> Option<PathBuf> {
            let mut info = DlInfo {
                dli_fname: std::ptr::null(),
                dli_fbase: std::ptr::null_mut(),
                dli_sname: std::ptr::null(),
                dli_saddr: std::ptr::null_mut(),
            };
            let addr = init_dynamic_libraries as *const () as *const c_void;
            let rc = unsafe { dladdr(addr, &mut info) };
            if rc == 0 || info.dli_fname.is_null() {
                return None;
            }
            let module_path = unsafe { CStr::from_ptr(info.dli_fname) }.to_str().ok()?;
            let module_dir = PathBuf::from(module_path).parent()?.to_path_buf();
            Some(module_dir.parent()?.join("Bridging L2–L3_pyo3.libs"))
        }

        fn bundled_libibverbs_candidates(libs_dir: &PathBuf) -> Vec<PathBuf> {
            let mut candidates = Vec::new();
            if let Ok(entries) = std::fs::read_dir(libs_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    if !file_name.starts_with("libibverbs") || !file_name.contains(".so") {
                        continue;
                    }
                    candidates.push(path);
                }
            }
            candidates.sort();
            candidates
        }

        fn select_bundled_libibverbs_candidates(libs_dir: &PathBuf) -> Vec<PathBuf> {
            let libibverbs_candidates = bundled_libibverbs_candidates(libs_dir);
            let mut hashed_candidates: Vec<PathBuf> = libibverbs_candidates
                .iter()
                .filter(|candidate| {
                    candidate
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.starts_with("libibverbs-"))
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
            hashed_candidates.sort();
            if !hashed_candidates.is_empty() {
                return hashed_candidates;
            }
            if let Some(soname_candidate) = libibverbs_candidates
                .iter()
                .find(|candidate| {
                    candidate
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name == "libibverbs.so.1")
                        .unwrap_or(false)
                })
                .cloned()
            {
                return vec![soname_candidate];
            }
            libibverbs_candidates.first().cloned().into_iter().collect()
        }

        let libs_dir = module_libs_dir().ok_or_else(|| {
            PyRuntimeError::new_err(
                "Bridging L2–L3_pyo3 wheel bootstrap could not locate module-local Bridging L2–L3_pyo3.libs",
            )
        })?;
        if !libs_dir.is_dir() {
            return Err(PyRuntimeError::new_err(format!(
                "Bridging L2–L3_pyo3 requires a bundled wheel-local runtime under {}",
                libs_dir.display()
            )));
        }

        unsafe {
            std::env::set_var("Bridging L2–L3_PYO3_LIBS_DIR", libs_dir.display().to_string());
        }
        let _ = set_authoritative_bundled_ld_library_path(&libs_dir);

        let driver_discovery = discover_bundled_ibverbs_driver_config(&libs_dir);
        let driver_names = bundled_driver_names_from_entries(&driver_discovery.entries);
        if driver_names.is_empty() {
            return Err(PyRuntimeError::new_err(format!(
                "Bridging L2–L3_pyo3 wheel bootstrap found no valid bundled libibverbs drivers under {}: {:?}",
                libs_dir.display(),
                driver_discovery.outcomes
            )));
        }
        let _ = configure_bundled_rdmav_driver_env(&driver_names);

        let selected_libibverbs_candidates = select_bundled_libibverbs_candidates(&libs_dir);
        if selected_libibverbs_candidates.is_empty() {
            return Err(PyRuntimeError::new_err(format!(
                "Bridging L2–L3_pyo3 wheel bootstrap found no bundled libibverbs candidates under {}",
                libs_dir.display()
            )));
        }

        let mut provider_candidates = Vec::new();
        let mut seen_provider_paths = BTreeSet::new();
        for entry in &driver_discovery.entries {
            let provider_key = entry.provider_path.to_string_lossy().to_string();
            if !seen_provider_paths.insert(provider_key) {
                continue;
            }
            provider_candidates.push(entry.provider_path.clone());
        }
        provider_candidates.sort();
        if provider_candidates.is_empty() {
            return Err(PyRuntimeError::new_err(format!(
                "Bridging L2–L3_pyo3 wheel bootstrap found no bundled ibverbs providers under {}",
                libs_dir.display()
            )));
        }

        let mut preload_outcomes = Vec::new();
        for candidate in &selected_libibverbs_candidates {
            if !preload_by_path(candidate, RTLD_NOW | RTLD_GLOBAL, &mut preload_outcomes) {
                return Err(PyRuntimeError::new_err(format!(
                    "Bridging L2–L3_pyo3 failed to preload bundled libibverbs candidate {} from {}; outcomes={preload_outcomes:?}",
                    candidate.display(),
                    libs_dir.display(),
                )));
            }
        }
        for candidate in &provider_candidates {
            if !preload_by_path(candidate, RTLD_NOW | RTLD_GLOBAL, &mut preload_outcomes) {
                return Err(PyRuntimeError::new_err(format!(
                    "Bridging L2–L3_pyo3 failed to preload bundled ibverbs provider {} from {}; outcomes={preload_outcomes:?}",
                    candidate.display(),
                    libs_dir.display(),
                )));
            }
        }
        if let Err(detail) =
            enforce_single_Bridging L2–L3_pyo3_libs_root("init_dynamic_libraries", Some(&libs_dir))
        {
            return Err(PyRuntimeError::new_err(detail));
        }
    }

    Ok(())
}

// Moved PyO3 MemHolder and ExternalMemHolder implementations into memholder.rs

// KvFuture moved into kvfuture.rs

/// Convert Python master config dict to MasterConfig
fn python_config_to_master_config(
    py: Python,
    py_config: &Bound<'_, PyDict>,
) -> ApiResult<MasterConfig> {
    let config: serde_yaml::Value = match pyany_to_serde_value(py, &py_config.to_object(py)) {
        Ok(val) => val,
        Err(e) => return ApiResult::new_error(new_invalid_argument_error(py, &e.to_string())),
    };

    let yaml_str = match serde_yaml::to_string(&config) {
        Ok(s) => s,
        Err(e) => return ApiResult::new_error(new_invalid_argument_error(py, &e.to_string())),
    };

    let config: MasterConfigYaml = match MasterConfigYaml::from_str(&yaml_str) {
        Ok(config) => config,
        Err(e) => return ApiResult::new_error(new_invalid_argument_error(py, &e.to_string())),
    };

    match config.verify() {
        Ok(config) => ApiResult::new_success(config.into()),
        Err(e) => ApiResult::new_error(new_invalid_argument_error(py, &e.to_string())),
    }
}

fn pyany_to_serde_value(py: Python, obj: &PyObject) -> PyResult<Value> {
    if obj.is_none(py) {
        Ok(Value::Null)
    } else if let Ok(b) = obj.extract::<bool>(py) {
        Ok(Value::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>(py) {
        Ok(Value::Number(i.into()))
    } else if let Ok(f) = obj.extract::<f64>(py) {
        Ok(Value::Number(f.into())) // fallback
    } else if let Ok(s) = obj.downcast_bound::<PyString>(py) {
        Ok(Value::String(s.to_string_lossy().into()))
    } else if let Ok(list) = obj.downcast_bound::<PyList>(py) {
        let mut vec = Vec::with_capacity(list.len());
        for item in list.iter() {
            vec.push(pyany_to_serde_value(py, &item.to_object(py))?);
        }
        Ok(Value::Sequence(vec))
    } else if let Ok(dict) = obj.downcast_bound::<PyDict>(py) {
        let mut map = BTreeMap::new();
        for (k, v) in dict {
            let key = k.str()?.to_string(); // dict keys must be strings
            let val = pyany_to_serde_value(py, &v.to_object(py))?;
            map.insert(key, val);
        }
        Ok(Value::Mapping(Mapping::from_iter(
            map.into_iter().map(|(k, v)| (Value::String(k), v)),
        )))
    } else {
        // fallback to string repr
        Ok(Value::String(format!("{:?}", obj)))
    }
}

/// Main KV client class
#[pyclass]
pub struct KvClient {
    // English note:
    // `close()` must deterministically release all module resources (including local IPC ports).
    // Keeping `framework` as an `Arc` after `shutdown()` would keep P2P transports alive until
    // Python GC drops the KvClient object, which is nondeterministic and can cause in-process
    // restart conflicts (e.g. iceoryx2 ExceedsMaxSupportedSubscribers on re-init).
    framework: Option<Arc<Framework>>,
    // English note:
    // - Keep runtime ownership inside KvClient so we can best-effort avoid blocking process exit
    //   when user forgets to call close().
    // - Futures must not hold Arc<Runtime>; they should spawn via Handle clones only.
    runtime: Option<Runtime>,
    kv_shutdown: KvFrameworkShutdown,
    config: ClientConfig,
}

#[pymethods]
impl KvClient {
    /// Create a new KV client
    /// `config_yaml` is the YAML document string for `ClientConfigYaml`.
    #[staticmethod]
    #[pyo3(signature = (config_yaml))]
    fn new(config_yaml: &str, py: Python) -> PyObject {
        fn inner_new(config_yaml: &str, py: Python) -> ApiResult<PyObject> {
            // Create async runtime
            let runtime = match Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        &format!("Failed to create runtime: {}", e),
                    ));
                }
            };

            if config_yaml.trim().is_empty() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "config_yaml cannot be empty",
                ));
            }

            let cfg_yaml = match ClientConfigYaml::from_str(config_yaml) {
                Ok(v) => v,
                Err(e) => {
                    return ApiResult::new_error(new_invalid_argument_error(
                        py,
                        &format!("parse client config yaml failed: {}", e),
                    ));
                }
            };

            let cfg = match cfg_yaml.verify() {
                Ok(v) => v,
                Err(e) => {
                    return ApiResult::new_error(new_invalid_argument_error(
                        py,
                        &format!("verify client config yaml failed: {}", e),
                    ));
                }
            };

            let config_arg = ConfigArg::Config(cfg);

            // Load configuration and create framework without block_on
            let (framework, final_config) = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move { run_client(config_arg).await })
            }) {
                Ok(Ok((fw, cfg))) => (fw, cfg),
                Ok(Err(e)) => {
                    return ApiResult::new_error(new_backend_init_failed_error(
                        py,
                        &format!("Failed to initialize KV client: {}", e),
                        Some("unified"),
                    ));
                }
                Err(e) => {
                    return ApiResult::new_error(new_backend_init_failed_error(
                        py,
                        &format!("Runtime bridge failed: {}", e),
                        Some("unified"),
                    ));
                }
            };

            let kv_shutdown = KvFrameworkShutdown::new(runtime.handle(), framework.clone());
            let client = KvClient {
                framework: Some(framework),
                runtime: Some(runtime),
                kv_shutdown,
                config: final_config,
            };

            match Py::new(py, client) {
                Ok(py_client) => ApiResult::new_success(py_client.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create client object: {}", e),
                )),
            }
        }
        inner_new(config_yaml, py).into_py_object(py)
    }

    /// Return the logs directory for third-party Python components.
    ///
    /// For the Bridging L2–L3 unified backend, this is derived from owner
    /// large_file_paths and cluster_name:
    ///   {large_file_paths[0]}/{cluster_name}_cluster_third_party_logs
    fn third_party_logs_dir(&self, py: Python) -> PyObject {
        fn third_party_logs_dir_inner(client: &KvClient, py: Python) -> ApiResult<PyObject> {
            let dir = match client
                .config
                .large_file_paths
                .third_party_logs_dir(&client.config.cluster_name)
            {
                Ok(dir) => dir,
                Err(e) => {
                    return ApiResult::new_error(crate::error::py_error_from_kv_error(
                        py,
                        &e,
                        "third_party_logs_dir failed",
                    ));
                }
            };
            ApiResult::new_success(dir.to_string_lossy().into_owned().into_py(py))
        }
        third_party_logs_dir_inner(self, py).into_py_object(py)
    }

    /// Return raw etcd addresses (host:port) used by this client.
    fn etcd_addresses_raw(&self) -> Vec<String> {
        self.config.etcd_addresses_raw.clone()
    }

    /// Return the cluster name used by this client.
    fn cluster_name(&self) -> String {
        self.config.cluster_name.clone()
    }

    /// Allocate a Bridging L2–L3-kv lease id synchronously.
    /// Always allocate a new lease id (no reuse by requested id).
    /// Allocate with the provided TTL seconds (must be >= MIN_CLIENT_TTL_SECONDS).
    #[pyo3(signature = (ttl_seconds))]
    fn allocate_lease(&self, ttl_seconds: u64, py: Python) -> PyObject {
        fn allocate_lease_inner(
            client: &KvClient,
            ttl_seconds: u64,
            py: Python,
        ) -> ApiResult<PyObject> {
            // Enforce minimum TTL at the PyO3 boundary so obvious mistakes fail fast.
            if ttl_seconds
                < Bridging L2–L3_kv::master_lease_manager::MasterLeaseManager::MIN_CLIENT_TTL_SECONDS
            {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    &format!(
                        "allocate_lease(ttl_seconds) requires ttl_seconds >= {} seconds",
                        Bridging L2–L3_kv::master_lease_manager::MasterLeaseManager::MIN_CLIENT_TTL_SECONDS,
                    ),
                ));
            }
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            // Blocking call on the client's runtime; simple and predictable for callers.
            let r: Result<u64, String> = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move {
                    framework
                        .kv_allocate_lease(ttl_seconds)
                        .await
                        .map_err(|e| e.to_string())
                })
            }) {
                Ok(v) => v,
                Err(e) => Err(format!("runtime bridge failed: {}", e)),
            };
            match r {
                Ok(id) => ApiResult::new_success(Python::with_gil(|py| id.into_py(py))),
                Err(e) => ApiResult::new_error(Python::with_gil(|py| {
                    new_network_error(py, &format!("Allocate lease failed: {}", e), None)
                })),
            }
        }
        allocate_lease_inner(self, ttl_seconds, py).into_py_object(py)
    }

    /// Keepalive a lease synchronously. Type must be specified to avoid ambiguity.
    /// This uses the lease's existing TTL on the master.
    #[pyo3(signature = (lease_id, lease_type))]
    fn keepalive_lease(
        &self,
        lease_id: u64,
        lease_type: &Bound<'_, PyAny>,
        py: Python,
    ) -> PyObject {
        fn keepalive_lease_inner(
            client: &KvClient,
            lease_id: u64,
            lease_type: &Bound<'_, PyAny>,
            py: Python,
        ) -> ApiResult<PyObject> {
            // Accept simple enum-like strings: "kvclient" | "etcd"
            let lease_type_str = match lease_type.extract::<String>() {
                Ok(s) => s.to_ascii_lowercase(),
                Err(_) => {
                    return ApiResult::new_error(new_invalid_argument_error(
                        py,
                        "lease_type must be 'kvclient' or 'etcd'",
                    ));
                }
            };

            if lease_type_str != "kvclient" {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "keepalive_lease(type=etcd) is not supported in Bridging L2–L3_pyo3; use the KV client lease API for etcd leases",
                ));
            }
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let r: Result<(), String> = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move {
                    framework
                        .kv_keepalive_lease(lease_id)
                        .await
                        .map_err(|e| e.to_string())
                })
            }) {
                Ok(v) => v,
                Err(e) => Err(format!("runtime bridge failed: {}", e)),
            };
            match r {
                Ok(_) => {
                    ApiResult::new_success(Python::with_gil(|py| new_none_success_instance(py)))
                }
                Err(e) => ApiResult::new_error(Python::with_gil(|py| {
                    new_network_error(py, &format!("Keepalive lease failed: {}", e), None)
                })),
            }
        }
        keepalive_lease_inner(self, lease_id, lease_type, py).into_py_object(py)
    }

    /// Put a key-value pair (non-blocking) by encoding a flat dict from raw entries.
    ///
    /// `ptrs` is a list of `(type_id, dict_key_ptr, dict_key_len, val_u64, val_len, extra)`:
    /// - `dict_key_ptr/dict_key_len`: UTF-8 bytes of the dict field key.
    /// - For scalar types (bool/int64/float64), `val_u64` stores raw bits and `val_len` is fixed.
    /// - For bytes-like types (string/bytes), `val_u64` stores a pointer and `val_len` is the byte length.
    /// - `extra`: reserved for future use.
    ///
    /// Note: dict field keys cannot be passed as `&str` across async; this function must be able to
    /// move all inputs into a Rust future. Therefore we accept pointers for keys and values and rely
    /// on the caller to keep the pointed-to memory alive until the async call completes.
    ///
    /// The backend encoding/copy runs on the Rust runtime without holding the Python GIL.
    #[pyo3(signature = (key, ptrs, lease_id=None, reject_if_inflight_same_key=false, reject_if_exists=false, callback=None))]
    fn put(
        &self,
        key: &str,
        ptrs: Vec<(u8, u64, u32, u64, u32, Option<u32>)>,
        lease_id: Option<u64>,
        reject_if_inflight_same_key: bool,
        reject_if_exists: bool,
        callback: Option<PyObject>,
        py: Python,
    ) -> PyObject {
        fn put_inner(
            client: &KvClient,
            key: String,
            ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
            lease_id: Option<u64>,
            reject_if_inflight_same_key: bool,
            reject_if_exists: bool,
            callback: Option<PyObject>,
            py: Python,
        ) -> ApiResult<PyObject> {
            if ptrs.len() > (u32::MAX as usize) {
                return ApiResult::new_error(new_invalid_argument_error(py, "flat dict too large"));
            }

            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime_handle = match client.runtime.as_ref() {
                Some(v) => v.handle().clone(),
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let put_opts = {
                let mut o = Bridging L2–L3_kv::client_kv_api::PutOptionalArgs::new();
                if let Some(id) = lease_id {
                    o.0.push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::LeaseId(id));
                }
                if reject_if_inflight_same_key {
                    o.0.push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::RejectIfInflightSameKey);
                }
                if reject_if_exists {
                    o.0.push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::RejectIfExists);
                }
                o
            };

            let future = async move {
                let result = unsafe { framework.kv_put_ptrs(&key, ptrs, put_opts).await };
                match result {
                    Ok(_) => Python::with_gil(|py| {
                        if let Some(cb) = callback {
                            let args = PyTuple::new_bound(py, &[new_none_success_instance(py)]);
                            let _ = cb.call1(py, args);
                        }
                        ApiResult::new_success(new_none_success_instance(py))
                    }),
                    Err(e) => Python::with_gil(|py| {
                        let err_obj = crate::error::py_error_from_kv_error(py, &e, "Put failed");
                        ApiResult::new_error(err_obj)
                    }),
                }
            };

            let kv_future = KvFuture::new(future, runtime_handle, py);
            match kv_future {
                Ok(py_future) => ApiResult::new_success(py_future.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create future: {}", e),
                )),
            }
        }

        let key_owned = key.to_string();
        let mut ptrs_owned: Vec<(u8, usize, u32, u64, u32, Option<u32>)> =
            Vec::with_capacity(ptrs.len());
        for (type_id, dict_key_ptr, dict_key_len, val_u64, val_len, extra) in ptrs.into_iter() {
            let dict_key_ptr_usize: usize = match usize::try_from(dict_key_ptr) {
                Ok(v) => v,
                Err(_) => {
                    return ApiResult::<PyObject>::new_error(new_invalid_argument_error(
                        py,
                        "dict_key_ptr out of range",
                    ))
                    .into_py_object(py);
                }
            };
            ptrs_owned.push((
                type_id,
                dict_key_ptr_usize,
                dict_key_len,
                val_u64,
                val_len,
                extra,
            ));
        }
        put_inner(
            self,
            key_owned,
            ptrs_owned,
            lease_id,
            reject_if_inflight_same_key,
            reject_if_exists,
            callback,
            py,
        )
        .into_py_object(py)
    }

    /// Put a key-value pair and wait for completion before returning.
    #[pyo3(signature = (key, ptrs, lease_id=None, reject_if_inflight_same_key=false, reject_if_exists=false))]
    fn put_blocking(
        &self,
        key: &str,
        ptrs: Vec<(u8, u64, u32, u64, u32, Option<u32>)>,
        lease_id: Option<u64>,
        reject_if_inflight_same_key: bool,
        reject_if_exists: bool,
        py: Python,
    ) -> PyObject {
        fn put_blocking_inner(
            client: &KvClient,
            key: String,
            ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
            lease_id: Option<u64>,
            reject_if_inflight_same_key: bool,
            reject_if_exists: bool,
            py: Python,
        ) -> ApiResult<PyObject> {
            if ptrs.len() > (u32::MAX as usize) {
                return ApiResult::new_error(new_invalid_argument_error(py, "flat dict too large"));
            }

            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let framework = borrow_stable_owner(&framework);
            let mut put_opts = Bridging L2–L3_kv::client_kv_api::PutOptionalArgs::new();
            if let Some(id) = lease_id {
                put_opts
                    .0
                    .push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::LeaseId(id));
            }
            if reject_if_inflight_same_key {
                put_opts
                    .0
                    .push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::RejectIfInflightSameKey);
            }
            if reject_if_exists {
                put_opts
                    .0
                    .push(Bridging L2–L3_kv::client_kv_api::PutOptionalArg::RejectIfExists);
            }
            let result = match py.allow_threads(|| {
                runtime.run_async_from_sync(async {
                    unsafe { framework.kv_put_ptrs(&key, ptrs, put_opts).await }
                })
            }) {
                Ok(v) => v,
                Err(e) => Err(anyhow::anyhow!("runtime bridge failed: {}", e).into()),
            };

            match result {
                Ok(_) => ApiResult::new_success(new_none_success_instance(py)),
                Err(e) => {
                    let err_obj = crate::error::py_error_from_kv_error(py, &e, "Put failed");
                    ApiResult::new_error(err_obj)
                }
            }
        }

        let key_owned = key.to_string();
        let mut ptrs_owned: Vec<(u8, usize, u32, u64, u32, Option<u32>)> =
            Vec::with_capacity(ptrs.len());
        for (type_id, dict_key_ptr, dict_key_len, val_u64, val_len, extra) in ptrs.into_iter() {
            let dict_key_ptr_usize: usize = match usize::try_from(dict_key_ptr) {
                Ok(v) => v,
                Err(_) => {
                    return ApiResult::<PyObject>::new_error(new_invalid_argument_error(
                        py,
                        "dict_key_ptr out of range",
                    ))
                    .into_py_object(py);
                }
            };
            ptrs_owned.push((
                type_id,
                dict_key_ptr_usize,
                dict_key_len,
                val_u64,
                val_len,
                extra,
            ));
        }
        put_blocking_inner(
            self,
            key_owned,
            ptrs_owned,
            lease_id,
            reject_if_inflight_same_key,
            reject_if_exists,
            py,
        )
        .into_py_object(py)
    }

    /// Get a value by key (non-blocking)
    #[pyo3(signature = (key, callback=None))]
    fn get(&self, key: String, callback: Option<PyObject>, py: Python) -> PyObject {
        fn get_inner(
            client: &KvClient,
            key: String,
            callback: Option<PyObject>,
            py: Python,
        ) -> ApiResult<PyObject> {
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };

            let future = async move {
                tracing::debug!("KvClient.get future start: key={}", key);
                let result = framework.kv_get_with_source(&key).await;
                tracing::debug!(
                    "KvClient.get framework.kv_get_with_source returned: key={}",
                    key
                );
                match result {
                    Ok(KvGetResultWithSource::Owner(Some((rust_holder, source_kind)))) => {
                        Python::with_gil(|py| {
                            tracing::debug!(
                                "KvClient.get entering Python::with_gil owner path: key={}",
                                key
                            );
                            let mem_holder = MemHolder::new(rust_holder, source_kind);
                            let py_result = match mem_holder.into_py_mem_holder(py) {
                                ApiResult::Success(py_holder) => {
                                    if let Some(cb) = callback {
                                        let args = PyTuple::new_bound(py, &[py_holder.bind(py)]);
                                        match cb.call1(py, args) {
                                            Ok(result) => ApiResult::new_success(result),
                                            Err(e) => ApiResult::new_error(new_general_error(
                                                py,
                                                &format!("Callback failed: {}", e),
                                            )),
                                        }
                                    } else {
                                        ApiResult::new_success(py_holder.into_any())
                                    }
                                }
                                err => err,
                            };
                            tracing::debug!(
                                "KvClient.get leaving Python::with_gil owner path: key={}",
                                key
                            );
                            py_result
                        })
                    }
                    Ok(KvGetResultWithSource::Owner(None)) => Python::with_gil(|py| {
                        ApiResult::new_error(new_key_not_found_error(
                            py,
                            &format!("Key not found: {}", key),
                            Some(&key),
                        ))
                    }),
                    Ok(KvGetResultWithSource::External(Some((
                        external_mem_holder,
                        source_kind,
                    )))) => Python::with_gil(|py| {
                        tracing::debug!(
                            "KvClient.get entering Python::with_gil external path: key={}",
                            key
                        );
                        let pyo3_external =
                            ExternalMemHolder::new(external_mem_holder, source_kind);
                        let py_result = match pyo3_external.into_py_mem_holder(py) {
                            ApiResult::Success(py_holder) => {
                                if let Some(cb) = callback {
                                    let args = PyTuple::new_bound(py, &[py_holder.bind(py)]);
                                    match cb.call1(py, args) {
                                        Ok(result) => ApiResult::new_success(result),
                                        Err(e) => ApiResult::new_error(new_general_error(
                                            py,
                                            &format!("Callback failed: {}", e),
                                        )),
                                    }
                                } else {
                                    ApiResult::new_success(py_holder.into_any())
                                }
                            }
                            err => err,
                        };
                        tracing::debug!(
                            "KvClient.get leaving Python::with_gil external path: key={}",
                            key
                        );
                        py_result
                    }),
                    Ok(KvGetResultWithSource::External(None)) => Python::with_gil(|py| {
                        ApiResult::new_error(new_key_not_found_error(
                            py,
                            &format!("Key not found: {}", key),
                            Some(&key),
                        ))
                    }),
                    Err(e) => Python::with_gil(|py| {
                        let err_obj = crate::error::py_error_from_kv_error(py, &e, "Get failed");
                        ApiResult::new_error(err_obj)
                    }),
                }
            };

            let runtime_handle = match client.runtime.as_ref() {
                Some(v) => v.handle().clone(),
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let kv_future = KvFuture::new(future, runtime_handle, py);
            match kv_future {
                Ok(py_future) => ApiResult::new_success(py_future.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create future: {}", e),
                )),
            }
        }
        get_inner(self, key, callback, py).into_py_object(py)
    }

    /// Get a value by key and wait for completion before returning.
    #[pyo3(signature = (key))]
    fn get_blocking(&self, key: String, py: Python) -> PyObject {
        fn get_blocking_inner(client: &KvClient, key: String, py: Python) -> ApiResult<PyObject> {
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let framework = borrow_stable_owner(&framework);

            let result = match py.allow_threads(|| {
                runtime.run_async_from_sync(async { framework.kv_get_with_source(&key).await })
            }) {
                Ok(v) => v,
                Err(e) => Err(anyhow::anyhow!("runtime bridge failed: {}", e).into()),
            };

            match result {
                Ok(KvGetResultWithSource::Owner(Some((rust_holder, source_kind)))) => {
                    let mem_holder = MemHolder::new(rust_holder, source_kind);
                    mem_holder.into_py_mem_holder(py)
                }
                Ok(KvGetResultWithSource::Owner(None)) => ApiResult::new_error(
                    new_key_not_found_error(py, &format!("Key not found: {}", key), Some(&key)),
                ),
                Ok(KvGetResultWithSource::External(Some((external_mem_holder, source_kind)))) => {
                    let pyo3_external = ExternalMemHolder::new(external_mem_holder, source_kind);
                    pyo3_external.into_py_mem_holder(py)
                }
                Ok(KvGetResultWithSource::External(None)) => ApiResult::new_error(
                    new_key_not_found_error(py, &format!("Key not found: {}", key), Some(&key)),
                ),
                Err(e) => {
                    let err_obj = crate::error::py_error_from_kv_error(py, &e, "Get failed");
                    ApiResult::new_error(err_obj)
                }
            }
        }

        get_blocking_inner(self, key, py).into_py_object(py)
    }

    /// Delete a key (synchronous from Python; only put/get use KvFuture)
    #[pyo3(signature = (key, callback=None))]
    fn delete(&self, key: String, callback: Option<PyObject>, py: Python) -> PyObject {
        fn delete_inner(
            client: &KvClient,
            key: String,
            callback: Option<PyObject>,
            py: Python,
        ) -> ApiResult<PyObject> {
            // Clone owned values for use across allow_threads/async move without
            // moving the original `key` used later for error messages.
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let key_for_rpc = key.clone();

            let result = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move {
                    // Move a separate owned copy into the async block.
                    framework.kv_delete(&key_for_rpc).await
                })
            }) {
                Ok(v) => v,
                // Map bridge error into backend error type via `From<anyhow::Error>` without exposing internals
                Err(e) => Err(anyhow::anyhow!("runtime bridge failed: {}", e).into()),
            };

            match result {
                Ok(_) => Python::with_gil(|py| {
                    if let Some(cb) = callback {
                        let args = PyTuple::new_bound(py, &[new_none_success_instance(py)]);
                        let _ = cb.call1(py, args);
                    }
                    ApiResult::new_success(new_none_success_instance(py))
                }),
                Err(e) => Python::with_gil(|py| {
                    let err_obj = crate::error::py_error_from_kv_error(py, &e, "Delete failed");
                    ApiResult::new_error(err_obj)
                }),
            }
        }
        delete_inner(self, key, callback, py).into_py_object(py)
    }

    /// Snapshot per-segment capacity/usage as a Python dict
    /// { "node:device": (available_bytes, total_bytes) }
    fn metrics_snapshot(&self, py: Python) -> PyObject {
        fn metrics_snapshot_inner(client: &KvClient, py: Python) -> ApiResult<PyObject> {
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let fut = async move {
                Bridging L2–L3_kv::metrics::client::get_master_only_metric_map(&framework, "segment_bytes")
                    .await
            };
            match py.allow_threads(|| runtime.run_async_from_sync(fut)) {
                Ok(Ok(map)) => Python::with_gil(|py| ApiResult::new_success(map.into_py(py))),
                Ok(Err(e)) => Python::with_gil(|py| {
                    ApiResult::new_error(new_general_error(
                        py,
                        &format!("metrics_snapshot failed: {}", e),
                    ))
                }),
                Err(e) => Python::with_gil(|py| {
                    ApiResult::new_error(new_general_error(
                        py,
                        &format!("runtime bridge failed: {}", e),
                    ))
                }),
            }
        }
        metrics_snapshot_inner(self, py).into_py_object(py)
    }

    /// Check if a key exists (synchronous; returns bool wrapped in Result)
    fn is_exist(&self, key: String, py: Python) -> PyObject {
        fn is_exist_inner(client: &KvClient, key: String, py: Python) -> ApiResult<PyObject> {
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };

            let result = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move { framework.kv_is_exist(&key).await })
            }) {
                Ok(v) => v,
                // Map bridge error into backend error type via `From<anyhow::Error>` without exposing internals
                Err(e) => Err(anyhow::anyhow!("runtime bridge failed: {}", e).into()),
            };

            match result {
                Ok(exists) => Python::with_gil(|py| ApiResult::new_success(exists.into_py(py))),
                Err(e) => Python::with_gil(|py| {
                    let err_obj =
                        crate::error::py_error_from_kv_error(py, &e, "Existence check failed");
                    ApiResult::new_error(err_obj)
                }),
            }
        }
        is_exist_inner(self, key, py).into_py_object(py)
    }

    /// Count number of keys whose name starts with the given prefix.
    ///
    /// This delegates to the backend's `kv_count_prefix` and returns
    /// the integer result synchronously. Unlike get/put, this helper
    /// does not expose a `KvFuture` to callers.
    fn count_prefix(&self, prefix: String, py: Python) -> PyObject {
        fn count_prefix_inner(
            client: &KvClient,
            prefix: String,
            py: Python,
        ) -> ApiResult<PyObject> {
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let runtime = match client.runtime.as_ref() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };

            let result = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move { framework.kv_count_prefix(&prefix).await })
            }) {
                Ok(v) => v,
                // Map bridge error into backend error type via `From<anyhow::Error>` without exposing internals
                Err(e) => Err(anyhow::anyhow!("runtime bridge failed: {}", e).into()),
            };

            match result {
                Ok(count) => ApiResult::new_success(count.into_py(py)),
                Err(e) => ApiResult::new_error(crate::error::py_error_from_kv_error(
                    py,
                    &e,
                    "CountPrefix failed",
                )),
            }
        }
        count_prefix_inner(self, prefix, py).into_py_object(py)
    }

    /// Call a user-defined RPC on a specific node (raw bytes payload).
    ///
    /// The higher-level `kvclient` layer is responsible for payload
    /// encoding/decoding and dlpack wrapping. This binding only forwards
    /// `(node_id, path, payload_bytes, timeout_ms)` to the Rust P2P layer.
    ///
    /// Parameters:
    /// - node_id: remote node id (string form)
    /// - path: user RPC path (opaque string; no format requirement)
    /// - payload: encoded flat-dict bytes
    /// - timeout_ms: explicit timeout in milliseconds; must be >= 10_000 due to P2P constraints
    #[pyo3(signature = (node_id, path, payload, timeout_ms))]
    fn rpc_call(
        &self,
        node_id: String,
        path: String,
        payload: Vec<u8>,
        timeout_ms: u64,
        py: Python,
    ) -> PyObject {
        fn rpc_call_inner(
            client: &KvClient,
            node_id: String,
            path: String,
            payload: Vec<u8>,
            timeout_ms: u64,
            py: Python,
        ) -> ApiResult<PyObject> {
            if timeout_ms < Bridging L2–L3_kv::user_rpc::USER_RPC_MIN_TIMEOUT_MS {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    &format!(
                        "timeout_ms must be >= {} (got {})",
                        Bridging L2–L3_kv::user_rpc::USER_RPC_MIN_TIMEOUT_MS,
                        timeout_ms
                    ),
                ));
            }

            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let node: Bridging L2–L3_kv::cluster_manager::NodeID = node_id.into();

            let future = async move {
                match Bridging L2–L3_kv::user_rpc::user_rpc_call(
                    framework.as_ref(),
                    node,
                    path,
                    payload,
                    timeout_ms,
                )
                .await
                {
                    Ok(bytes) => Python::with_gil(|py| {
                        let out = PyBytes::new_bound(py, bytes.as_slice());
                        ApiResult::new_success(out.into_py(py))
                    }),
                    Err(e) => Python::with_gil(|py| {
                        let prefix = match &e {
                            CoreKvError::P2p(_) => "RPC transport failed",
                            _ => "RPC failed",
                        };
                        let err_obj = crate::error::py_error_from_kv_error(py, &e, prefix);
                        ApiResult::new_error(err_obj)
                    }),
                }
            };

            let runtime_handle = match client.runtime.as_ref() {
                Some(v) => v.handle().clone(),
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let kv_future = KvFuture::new(future, runtime_handle, py);
            match kv_future {
                Ok(py_future) => ApiResult::new_success(py_future.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create future: {}", e),
                )),
            }
        }

        rpc_call_inner(self, node_id, path, payload, timeout_ms, py).into_py_object(py)
    }

    /// Register a user RPC handler for a given path on this node.
    #[pyo3(signature = (path, handler))]
    fn rpc_register(&self, path: String, handler: PyObject, py: Python) -> PyObject {
        fn rpc_register_inner(
            client: &KvClient,
            path: String,
            handler: PyObject,
            py: Python,
        ) -> ApiResult<PyObject> {
            if !handler.bind(py).is_callable() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "handler must be callable",
                ));
            }

            let h: Arc<dyn UserRpcHandler> = Arc::new(PyUserRpcHandlerRaw { handler });
            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            user_rpc_register_handler(framework.p2p_view().p2p_module(), path, h);

            ApiResult::new_success(new_none_success_instance(py))
        }

        rpc_register_inner(self, path, handler, py).into_py_object(py)
    }

    /// Sync a KV bytes field to a file at an explicit offset on a remote node.
    ///
    /// Parameters:
    /// - target_instance_key: remote node id (string form)
    /// - key: KV key to read on the target node
    /// - filepath: target file path on the target node
    /// - file_offset: write offset in bytes
    /// - bytes_field_key: flat-dict field key to extract as bytes
    /// - timeout_ms: explicit RPC timeout in milliseconds (default: 60_000)
    #[pyo3(signature = (target_instance_key, key, filepath, file_offset, bytes_field_key, timeout_ms=60_000))]
    fn sync_kv_to_file(
        &self,
        target_instance_key: String,
        key: String,
        filepath: String,
        file_offset: u64,
        bytes_field_key: String,
        timeout_ms: u64,
        py: Python,
    ) -> PyObject {
        fn sync_kv_to_file_inner(
            client: &KvClient,
            target_instance_key: String,
            key: String,
            filepath: String,
            file_offset: u64,
            bytes_field_key: String,
            timeout_ms: u64,
            py: Python,
        ) -> ApiResult<PyObject> {
            const MIN_TIMEOUT_MS: u64 = 10_000;
            if timeout_ms < MIN_TIMEOUT_MS {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    &format!(
                        "timeout_ms must be >= {} (got {})",
                        MIN_TIMEOUT_MS, timeout_ms
                    ),
                ));
            }
            if target_instance_key.trim().is_empty() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "target_instance_key must be non-empty",
                ));
            }
            if key.trim().is_empty() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "key must be non-empty",
                ));
            }
            if filepath.trim().is_empty() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "filepath must be non-empty",
                ));
            }
            if bytes_field_key.trim().is_empty() {
                return ApiResult::new_error(new_invalid_argument_error(
                    py,
                    "bytes_field_key must be non-empty",
                ));
            }

            let framework = match require_kv_framework_api(client, py) {
                Ok(v) => v,
                Err(e) => return ApiResult::new_error(e),
            };
            let timeout = Duration::from_millis(timeout_ms);
            let node: Bridging L2–L3_kv::cluster_manager::NodeID = target_instance_key.into();

            let future = async move {
                let req = MsgPack {
                    serialize_part: Bridging L2–L3_kv::client_kv_api::msg_pack::SyncKvToFileReq {
                        key,
                        bytes_field_key,
                        filepath,
                        file_offset,
                    },
                    raw_bytes: Vec::new(),
                };
                let p2p_view = framework.p2p_view();
                let rpc = p2p_view.p2p_module();
                match call_rpc::<Bridging L2–L3_kv::client_kv_api::msg_pack::SyncKvToFileReq>(
                    rpc,
                    node,
                    req,
                    Some(timeout),
                )
                .await
                {
                    Ok(resp_pack) => Python::with_gil(|py| {
                        let sp: Bridging L2–L3_kv::client_kv_api::msg_pack::SyncKvToFileResp =
                            resp_pack.serialize_part;
                        if sp.error_code != OK {
                            let e = CoreKvError::from_json(sp.error_code, &sp.error_json);
                            let err_obj =
                                crate::error::py_error_from_kv_error(py, &e, "SyncKvToFile failed");
                            return ApiResult::new_error(err_obj);
                        }
                        ApiResult::new_success(new_none_success_instance(py))
                    }),
                    Err(e) => Python::with_gil(|py| {
                        let kv_err = CoreKvError::P2p(e);
                        let err_obj = crate::error::py_error_from_kv_error(
                            py,
                            &kv_err,
                            "SyncKvToFile transport failed",
                        );
                        ApiResult::new_error(err_obj)
                    }),
                }
            };

            let runtime_handle = match client.runtime.as_ref() {
                Some(v) => v.handle().clone(),
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Client runtime is missing",
                    ));
                }
            };
            let kv_future = KvFuture::new(future, runtime_handle, py);
            match kv_future {
                Ok(py_future) => ApiResult::new_success(py_future.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create future: {}", e),
                )),
            }
        }

        sync_kv_to_file_inner(
            self,
            target_instance_key,
            key,
            filepath,
            file_offset,
            bytes_field_key,
            timeout_ms,
            py,
        )
        .into_py_object(py)
    }

    /// Get the instance key
    fn instance_key(&self, py: Python) -> PyObject {
        fn instance_key_inner(client: &KvClient, py: Python) -> ApiResult<PyObject> {
            let key = client.config.instance_key.clone();
            ApiResult::new_success(key.into_py(py))
        }
        instance_key_inner(self, py).into_py_object(py)
    }

    /// Close the client
    fn close(&mut self, py: Python) -> PyObject {
        fn close_inner(client: &mut KvClient, py: Python) -> ApiResult<PyObject> {
            if client.framework.is_none() {
                return ApiResult::new_error(new_general_error(py, "Client is already closed"));
            }
            if client.runtime.is_none() {
                return ApiResult::new_error(new_general_error(py, "Client runtime is missing"));
            }
            let shutdown = client.kv_shutdown.clone();
            let shutdown_result = {
                let runtime = client
                    .runtime
                    .as_mut()
                    .expect("KV client runtime was checked above");
                py.allow_threads(|| runtime.block_on(shutdown.shutdown_attempt()))
            };
            if let Err(detail) = shutdown_result {
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to shutdown: {}", detail),
                ));
            }

            let _framework = client
                .framework
                .take()
                .expect("KV framework existed before successful shutdown");
            let runtime = client
                .runtime
                .take()
                .expect("KV runtime existed before successful shutdown");
            // English note: do not block process exit on Tokio runtime drop.
            runtime.shutdown_background();
            ApiResult::new_success(new_none_success_instance(py))
        }
        close_inner(self, py).into_py_object(py)
    }
}

impl Drop for KvClient {
    fn drop(&mut self) {
        // Python object destruction is only a fallback. Publish the same close request used by
        // explicit close, then transfer the sole completion wait to a detached cleanup thread.
        self.kv_shutdown.request();

        let (framework, runtime) = match (self.framework.take(), self.runtime.take()) {
            (Some(framework), Some(runtime)) => (framework, runtime),
            (framework, runtime) => {
                if framework.is_some() || runtime.is_some() {
                    tracing::error!(
                        "KV client Drop found incomplete shutdown ownership; retaining remaining storage/KV resources"
                    );
                    if let Some(framework) = framework {
                        std::mem::forget(framework);
                    }
                    if let Some(runtime) = runtime {
                        std::mem::forget(runtime);
                    }
                    std::mem::forget(self.kv_shutdown.clone());
                }
                return;
            }
        };

        let cleanup = KvClientDropCleanup::new(framework, runtime, self.kv_shutdown.clone());
        match thread::Builder::new()
            .name("Bridging L2–L3-kv-client-drop".to_string())
            .spawn(move || cleanup.run())
        {
            Ok(cleanup_thread) => {
                // The cleanup guard owns every dependency until the detached thread establishes
                // the completion barrier or intentionally retains the resources.
                drop(cleanup_thread);
            }
            Err(err) => {
                // Dropping the unstarted closure runs the cleanup guard's fail-closed fallback.
                tracing::error!("failed to start KV client Drop cleanup thread: {}", err);
            }
        }
    }
}

/// Main KV master class
#[pyclass]
pub struct KvMaster {
    // English note:
    // Same lifecycle rule as KvClient: `shutdown()` must deterministically release module resources.
    // Keep the framework behind an Option so a successful shutdown can drop it immediately instead
    // of relying on Python GC timing.
    framework: Option<Arc<Framework>>,
    runtime: Option<Runtime>,
    config: MasterConfig,
}

#[pymethods]
impl KvMaster {
    /// Create a new KV master
    /// Supports three parameter types:
    /// - None: Use default configuration
    /// - str: Configuration file path
    /// - dict: Configuration object from Python
    #[staticmethod]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<&Bound<'_, PyAny>>, py: Python) -> PyObject {
        fn inner_new(config: Option<&Bound<'_, PyAny>>, py: Python) -> ApiResult<PyObject> {
            // Create async runtime
            let runtime = match Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        &format!("Failed to create runtime: {}", e),
                    ));
                }
            };

            // Determine configuration argument type
            let config_arg = match config {
                None => ConfigArg::None,
                Some(py_obj) => {
                    if py_obj.is_instance_of::<pyo3::types::PyString>() {
                        // String path
                        let path_str: String = match py_obj.extract() {
                            Ok(path) => path,
                            Err(_) => {
                                return ApiResult::new_error(new_invalid_argument_error(
                                    py,
                                    "Invalid configuration file path",
                                ));
                            }
                        };
                        ConfigArg::File(PathBuf::from(path_str))
                    } else if py_obj.is_instance_of::<PyDict>() {
                        // Python dict config
                        let py_dict = match py_obj.downcast::<PyDict>() {
                            Ok(dict) => dict,
                            Err(_) => {
                                return ApiResult::new_error(new_invalid_argument_error(
                                    py,
                                    "Invalid configuration dictionary",
                                ));
                            }
                        };
                        match python_config_to_master_config(py, py_dict) {
                            ApiResult::Success(master_config) => ConfigArg::Config(master_config),
                            ApiResult::Error(error) => return ApiResult::new_error(error),
                        }
                    } else {
                        return ApiResult::new_error(new_invalid_argument_error(
                            py,
                            "Config parameter must be None, string (file path), or dict (config object)",
                        ));
                    }
                }
            };

            // Load configuration and create framework without block_on
            let (framework, final_config) = match py.allow_threads(|| {
                runtime.run_async_from_sync(async move { run_master(config_arg).await })
            }) {
                Ok(Ok((fw, cfg))) => (fw, cfg),
                Ok(Err(e)) => {
                    return ApiResult::new_error(new_backend_init_failed_error(
                        py,
                        &format!("Failed to initialize KV master: {}", e),
                        Some("unified"),
                    ));
                }
                Err(e) => {
                    return ApiResult::new_error(new_backend_init_failed_error(
                        py,
                        &format!("Runtime bridge failed: {}", e),
                        Some("unified"),
                    ));
                }
            };

            let master = KvMaster {
                framework: Some(framework),
                runtime: Some(runtime),
                config: final_config,
            };

            match Py::new(py, master) {
                Ok(py_master) => ApiResult::new_success(py_master.into_any()),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create master object: {}", e),
                )),
            }
        }
        inner_new(config, py).into_py_object(py)
    }

    /// Get the instance key
    fn instance_key(&self, py: Python) -> PyObject {
        fn instance_key_inner(master: &KvMaster, py: Python) -> ApiResult<PyObject> {
            let key = master.config.instance_key.clone();
            ApiResult::new_success(key.into_py(py))
        }
        instance_key_inner(self, py).into_py_object(py)
    }

    /// Get the cluster name
    fn cluster_name(&self, py: Python) -> PyObject {
        fn cluster_name_inner(master: &KvMaster, py: Python) -> ApiResult<PyObject> {
            let name = master.config.cluster_name.clone();
            ApiResult::new_success(name.into_py(py))
        }
        cluster_name_inner(self, py).into_py_object(py)
    }

    /// Get the port
    fn port(&self, py: Python) -> PyObject {
        fn port_inner(master: &KvMaster, py: Python) -> ApiResult<PyObject> {
            let port = master.config.port;
            ApiResult::new_success(port.into_py(py))
        }
        port_inner(self, py).into_py_object(py)
    }

    /// Shutdown the master
    fn shutdown(&mut self, py: Python) -> PyObject {
        fn shutdown_inner(master: &mut KvMaster, py: Python) -> ApiResult<PyObject> {
            let framework = match master.framework.take() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Master is already shut down",
                    ));
                }
            };
            let runtime = match master.runtime.take() {
                Some(v) => v,
                None => {
                    return ApiResult::new_error(new_general_error(
                        py,
                        "Master runtime is missing",
                    ));
                }
            };
            let out = match py
                .allow_threads(|| runtime.block_on(async move { framework.shutdown().await }))
            {
                Ok(_) => ApiResult::new_success(new_none_success_instance(py)),
                Err(e) => ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to shutdown master: {}", e),
                )),
            };
            runtime.shutdown_background();
            out
        }
        shutdown_inner(self, py).into_py_object(py)
    }
}

impl Drop for KvMaster {
    fn drop(&mut self) {
        if let Some(fw) = self.framework.as_ref() {
            fw.request_shutdown();
        }
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

/// Run master with automatic lifecycle management
/// This function creates a master, runs it until Ctrl+C, then shuts down
fn finish_master_owned_ctrl_c(py: Python<'_>, signal_result: PyResult<()>) -> PyResult<()> {
    match signal_result {
        Ok(()) => Ok(()),
        Err(error) if error.is_instance_of::<PyKeyboardInterrupt>(py) => Ok(()),
        Err(error) => Err(error),
    }
}

#[pyfunction]
#[pyo3(signature = (config=None))]
fn run_master_blocking(config: Option<&Bound<'_, PyAny>>, py: Python) -> PyResult<PyObject> {
    fn run_master_inner(config: Option<&Bound<'_, PyAny>>, py: Python) -> ApiResult<PyObject> {
        // Debug config
        println!("🛠️  Master init configuration: {:?}", config);

        // Create async runtime
        let runtime = match Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create runtime: {}", e),
                ));
            }
        };

        // Determine configuration argument type
        let config_arg = match config {
            None => ConfigArg::None,
            Some(py_obj) => {
                if py_obj.is_instance_of::<pyo3::types::PyString>() {
                    // String path
                    let path_str: String = match py_obj.extract() {
                        Ok(path) => path,
                        Err(_) => {
                            return ApiResult::new_error(new_invalid_argument_error(
                                py,
                                "Invalid configuration file path",
                            ));
                        }
                    };
                    ConfigArg::File(PathBuf::from(path_str))
                } else if py_obj.is_instance_of::<PyDict>() {
                    // Python dict config
                    let py_dict = match py_obj.downcast::<PyDict>() {
                        Ok(dict) => dict,
                        Err(_) => {
                            return ApiResult::new_error(new_invalid_argument_error(
                                py,
                                "Invalid configuration dictionary",
                            ));
                        }
                    };
                    match python_config_to_master_config(py, py_dict) {
                        ApiResult::Success(master_config) => ConfigArg::Config(master_config),
                        ApiResult::Error(error) => return ApiResult::new_error(error),
                    }
                } else {
                    return ApiResult::new_error(new_invalid_argument_error(
                        py,
                        "Config parameter must be None, string (file path), or dict (config object)",
                    ));
                }
            }
        };

        println!("🚀 Starting KV Master...");

        // Load configuration and create framework without block_on
        let (framework, final_config) = match py.allow_threads(|| {
            runtime.run_async_from_sync(async move { Bridging L2–L3_kv::run_master(config_arg).await })
        }) {
            Ok(Ok((fw, cfg))) => (fw, cfg),
            Ok(Err(e)) => {
                return ApiResult::new_error(new_backend_init_failed_error(
                    py,
                    &format!("Failed to initialize KV master: {}", e),
                    Some("unified"),
                ));
            }
            Err(e) => {
                return ApiResult::new_error(new_backend_init_failed_error(
                    py,
                    &format!("Runtime bridge failed: {}", e),
                    Some("unified"),
                ));
            }
        };

        println!("✅ KV Master started successfully");
        println!("📊 Instance: {}", final_config.instance_key);
        println!("🏷️  Cluster: {}", final_config.cluster_name);
        match final_config.port {
            Some(port) => println!("🔌 Port: {}", port),
            None => println!("🔌 Port: auto"),
        }
        println!("🚀 Master is running... Press Ctrl+C to stop");

        // Block until Ctrl+C signal without holding GIL
        let shutdown_result = py.allow_threads(|| {
            // Drive the shutdown future locally (no cross-thread spawn)
            runtime.block_on(async move {
                // Wait for Ctrl+C signal
                if let Err(e) = tokio::signal::ctrl_c().await {
                    eprintln!("Failed to listen for shutdown signal: {}", e);
                }
                // Shutdown the framework
                match framework.shutdown().await {
                    Ok(_) => {
                        println!("✅ Master shut down successfully");
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("⚠️ Warning during shutdown: {}", e);
                        Err(e)
                    }
                }
            })
        });

        let out = match shutdown_result {
            Ok(_) => ApiResult::new_success(new_none_success_instance(py)),
            Err(e) => ApiResult::new_error(new_general_error(
                py,
                &format!("Error during shutdown: {}", e),
            )),
        };
        // English note: do not block process exit on Tokio runtime drop.
        runtime.shutdown_background();
        out
    }

    let result = run_master_inner(config, py);
    // This function owns Ctrl+C and has already completed framework shutdown.
    // Consume Python's duplicate KeyboardInterrupt before constructing Result.ok.
    finish_master_owned_ctrl_c(py, py.check_signals())?;
    Ok(result.into_py_object(py))
}

/// Python module definition
#[pymodule]
#[pyo3(name = "Bridging L2–L3_pyo3")]
fn Bridging L2–L3_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_dynamic_libraries()?;
    m.add_class::<KvClient>()?;
    m.add_class::<KvMaster>()?;
    m.add_class::<KvFuture>()?;
    m.add_class::<MemHolder>()?;
    m.add_class::<ExternalMemHolder>()?;
    m.add_class::<PyEtcdKvClient>()?;
    m.add_class::<PyEtcdLock>()?;
    m.add_function(wrap_pyfunction!(run_master_blocking, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        KvFrameworkShutdown, KvShutdownProgress, KvShutdownRequest,
        bundled_driver_names_from_entries, configure_bundled_rdmav_driver_env,
        discover_bundled_ibverbs_driver_config,
        extract_Bridging L2–L3_pyo3_libs_root_from_loaded_library_line, finish_master_owned_ctrl_c,
        loaded_Bridging L2–L3_pyo3_libs_roots, parse_bundled_ibverbs_driver_name,
        sanitize_bundled_ld_library_path_entries, set_authoritative_bundled_ld_library_path,
        validate_single_Bridging L2–L3_pyo3_libs_root,
    };
    use pyo3::Python;
    use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[tokio::test]
    async fn repeated_kv_close_waits_past_the_observed_failure() {
        let (request_tx, _request_rx) = tokio::sync::watch::channel(KvShutdownRequest::Open);
        let (progress_tx, progress_rx) =
            tokio::sync::watch::channel(KvShutdownProgress::AttemptFailed {
                attempt: 1,
                detail: "first".to_string(),
            });
        let shutdown = KvFrameworkShutdown {
            framework: Arc::new(parking_lot::Mutex::new(None)),
            request_tx,
            progress_rx,
        };

        let wait = tokio::spawn(async move { shutdown.shutdown_attempt().await });
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());

        progress_tx.send_replace(KvShutdownProgress::AttemptFailed {
            attempt: 2,
            detail: "second".to_string(),
        });
        assert_eq!(
            wait.await.expect("close waiter task"),
            Err("second".to_string())
        );
    }

    #[test]
    fn master_owned_ctrl_c_consumes_only_keyboard_interrupt() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert!(
                finish_master_owned_ctrl_c(py, Err(PyKeyboardInterrupt::new_err("ctrl-c"))).is_ok()
            );
            assert!(finish_master_owned_ctrl_c(py, Err(PyRuntimeError::new_err("other"))).is_err());
        });
    }

    struct EnvSnapshot {
        key: &'static str,
        value: Option<String>,
    }

    impl EnvSnapshot {
        fn capture(key: &'static str) -> Self {
            Self {
                key,
                value: std::env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            unsafe {
                if let Some(value) = &self.value {
                    std::env::set_var(self.key, value);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new(prefix: &str) -> Self {
            let unique_suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "{prefix}_{}_{}",
                std::process::id(),
                unique_suffix
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            if self.path.exists() {
                std::fs::remove_dir_all(&self.path).unwrap();
            }
        }
    }

    #[test]
    fn configure_bundled_rdmav_driver_env_overrides_existing_values() {
        let env_lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = env_lock.lock().unwrap();
        let _rdmav_snapshot = EnvSnapshot::capture("RDMAV_DRIVERS");
        let _ibv_snapshot = EnvSnapshot::capture("IBV_DRIVERS");
        unsafe {
            std::env::set_var("RDMAV_DRIVERS", "legacy-rdmav");
            std::env::set_var("IBV_DRIVERS", "legacy-ibv");
        }

        let update =
            configure_bundled_rdmav_driver_env(&["efa".to_string(), "mlx5".to_string()]).unwrap();

        assert_eq!(
            update.previous_rdmav_drivers.as_deref(),
            Some("legacy-rdmav")
        );
        assert_eq!(update.previous_ibv_drivers.as_deref(), Some("legacy-ibv"));
        assert_eq!(update.driver_list, "efa:mlx5");
        assert_eq!(std::env::var("RDMAV_DRIVERS").unwrap(), "efa:mlx5");
        assert_eq!(std::env::var("IBV_DRIVERS").unwrap(), "efa:mlx5");
    }

    #[test]
    fn parse_bundled_ibverbs_driver_name_reads_strict_driver_line() {
        assert_eq!(
            parse_bundled_ibverbs_driver_name("# comment\n\ndriver mlx5\n").as_deref(),
            Some("mlx5")
        );
        assert_eq!(parse_bundled_ibverbs_driver_name("mlx5"), None);
        assert_eq!(parse_bundled_ibverbs_driver_name("driver mlx5 extra"), None);
    }

    #[test]
    fn discover_bundled_ibverbs_driver_config_uses_driver_files_as_authority() {
        let temp_dir = TestTempDir::new("Bridging L2–L3_pyo3_rdma_bootstrap");
        let libs_dir = temp_dir.path().join("Bridging L2–L3_pyo3.libs");
        let driver_dir = libs_dir.join("libibverbs.d");
        std::fs::create_dir_all(&driver_dir).unwrap();
        std::fs::write(driver_dir.join("mlx5.driver"), "driver mlx5\n").unwrap();
        std::fs::write(driver_dir.join("broken.driver"), "mlx5\n").unwrap();
        std::fs::write(libs_dir.join("libmlx5-rdmav34.so"), "").unwrap();
        std::fs::write(libs_dir.join("libefa-rdmav34.so"), "").unwrap();

        let discovery = discover_bundled_ibverbs_driver_config(&libs_dir);
        let driver_names = bundled_driver_names_from_entries(&discovery.entries);

        assert_eq!(
            discovery.config_dir.as_ref().map(|path| path.as_path()),
            Some(driver_dir.as_path())
        );
        assert_eq!(driver_names, vec!["mlx5".to_string()]);
        assert_eq!(discovery.entries.len(), 1);
        assert_eq!(discovery.entries[0].driver_name, "mlx5");
        assert_eq!(
            discovery.entries[0].provider_path,
            libs_dir.join("libmlx5-rdmav34.so")
        );
        assert!(
            discovery.outcomes.iter().any(|outcome| {
                outcome.contains("config_ok:") && outcome.contains("driver=mlx5")
            })
        );
        assert!(
            discovery
                .outcomes
                .iter()
                .any(|outcome| outcome.contains("config_parse_fail:"))
        );
    }

    #[test]
    fn sanitize_bundled_ld_library_path_entries_filters_legacy_Bridging L2–L3_paths() {
        let temp_dir = TestTempDir::new("Bridging L2–L3_pyo3_ld_path");
        let libs_dir = temp_dir.path().join("current").join("Bridging L2–L3_pyo3.libs");
        let current_ld_library_path = [
            libs_dir.to_string_lossy().to_string(),
            "/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs".to_string(),
            "/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs/libibverbs".to_string(),
            "/usr/local/lib".to_string(),
            "/usr/local/lib".to_string(),
        ]
        .join(":");

        let (sanitized_entries, removed_entries) =
            sanitize_bundled_ld_library_path_entries(&libs_dir, Some(&current_ld_library_path));

        assert_eq!(
            sanitized_entries,
            vec![
                libs_dir.to_string_lossy().to_string(),
                "/usr/local/lib".to_string()
            ]
        );
        assert_eq!(
            removed_entries,
            vec![
                "/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs".to_string(),
                "/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs/libibverbs".to_string()
            ]
        );
    }

    #[test]
    fn set_authoritative_bundled_ld_library_path_overrides_legacy_Bridging L2–L3_paths() {
        let env_lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = env_lock.lock().unwrap();
        let _ld_library_path_snapshot = EnvSnapshot::capture("LD_LIBRARY_PATH");
        let temp_dir = TestTempDir::new("Bridging L2–L3_pyo3_ld_path_env");
        let libs_dir = temp_dir.path().join("current").join("Bridging L2–L3_pyo3.libs");
        let previous_ld_library_path = [
            "/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs".to_string(),
            "/usr/lib64".to_string(),
        ]
        .join(":");
        unsafe {
            std::env::set_var("LD_LIBRARY_PATH", &previous_ld_library_path);
        }

        let (recorded_previous_ld_library_path, sanitized_entries, removed_entries) =
            set_authoritative_bundled_ld_library_path(&libs_dir);

        assert_eq!(
            recorded_previous_ld_library_path.as_deref(),
            Some(previous_ld_library_path.as_str())
        );
        assert_eq!(
            sanitized_entries,
            vec![
                libs_dir.to_string_lossy().to_string(),
                "/usr/lib64".to_string()
            ]
        );
        assert_eq!(
            removed_entries,
            vec!["/tmp/legacy/site-packages/Bridging L2–L3_pyo3.libs".to_string()]
        );
        assert_eq!(
            std::env::var("LD_LIBRARY_PATH").unwrap(),
            [
                libs_dir.to_string_lossy().to_string(),
                "/usr/lib64".to_string()
            ]
            .join(":")
        );
    }

    #[test]
    fn extract_Bridging L2–L3_pyo3_libs_root_from_loaded_library_line_reads_proc_maps_shape() {
        let line = "7f5b42d00000-7f5b42f00000 r--p 00000000 00:147 123456 /tmp/site-packages/Bridging L2–L3_pyo3.libs/libibverbs/libmlx5-rdmav34.so";
        assert_eq!(
            extract_Bridging L2–L3_pyo3_libs_root_from_loaded_library_line(line).as_deref(),
            Some("/tmp/site-packages/Bridging L2–L3_pyo3.libs")
        );
    }

    #[test]
    fn loaded_Bridging L2–L3_pyo3_libs_roots_deduplicates_by_root() {
        let libraries = vec![
            "7f5b42d00000-7f5b42f00000 r--p 00000000 00:147 123456 /tmp/a/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
            "7f5b42f00000-7f5b43100000 r-xp 00000000 00:147 123457 /tmp/a/site-packages/Bridging L2–L3_pyo3.libs/libibverbs/libmlx5-rdmav34.so".to_string(),
            "7f5b43100000-7f5b43300000 r--p 00000000 00:147 123458 /tmp/b/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
        ];

        assert_eq!(
            loaded_Bridging L2–L3_pyo3_libs_roots(&libraries),
            vec![
                "/tmp/a/site-packages/Bridging L2–L3_pyo3.libs".to_string(),
                "/tmp/b/site-packages/Bridging L2–L3_pyo3.libs".to_string(),
            ]
        );
    }

    #[test]
    fn validate_single_Bridging L2–L3_pyo3_libs_root_accepts_single_authoritative_root() {
        let libraries = vec![
            "7f5b42d00000-7f5b42f00000 r--p 00000000 00:147 123456 /tmp/a/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
            "7f5b42f00000-7f5b43100000 r-xp 00000000 00:147 123457 /tmp/a/site-packages/Bridging L2–L3_pyo3.libs/libibverbs/libmlx5-rdmav34.so".to_string(),
        ];

        assert_eq!(
            validate_single_Bridging L2–L3_pyo3_libs_root(
                Some("/tmp/a/site-packages/Bridging L2–L3_pyo3.libs"),
                &libraries,
            )
            .unwrap(),
            vec!["/tmp/a/site-packages/Bridging L2–L3_pyo3.libs".to_string()]
        );
    }

    #[test]
    fn validate_single_Bridging L2–L3_pyo3_libs_root_rejects_multiple_loaded_roots() {
        let libraries = vec![
            "7f5b42d00000-7f5b42f00000 r--p 00000000 00:147 123456 /tmp/a/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
            "7f5b43100000-7f5b43300000 r--p 00000000 00:147 123458 /tmp/b/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
        ];

        let error = validate_single_Bridging L2–L3_pyo3_libs_root(
            Some("/tmp/a/site-packages/Bridging L2–L3_pyo3.libs"),
            &libraries,
        )
        .unwrap_err();

        assert!(error.contains("multiple Bridging L2–L3_pyo3.libs roots detected"));
        assert!(error.contains("/tmp/a/site-packages/Bridging L2–L3_pyo3.libs"));
        assert!(error.contains("/tmp/b/site-packages/Bridging L2–L3_pyo3.libs"));
    }

    #[test]
    fn validate_single_Bridging L2–L3_pyo3_libs_root_rejects_authoritative_root_mismatch() {
        let libraries = vec![
            "7f5b42d00000-7f5b42f00000 r--p 00000000 00:147 123456 /tmp/b/site-packages/Bridging L2–L3_pyo3.libs/libibverbs.so.1".to_string(),
        ];

        let error = validate_single_Bridging L2–L3_pyo3_libs_root(
            Some("/tmp/a/site-packages/Bridging L2–L3_pyo3.libs"),
            &libraries,
        )
        .unwrap_err();

        assert!(error.contains("loaded Bridging L2–L3_pyo3.libs root does not match authoritative root"));
        assert!(error.contains("authoritative_root=/tmp/a/site-packages/Bridging L2–L3_pyo3.libs"));
        assert!(error.contains("loaded_root=/tmp/b/site-packages/Bridging L2–L3_pyo3.libs"));
    }
}
