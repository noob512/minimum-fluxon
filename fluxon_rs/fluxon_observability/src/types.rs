#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluxonMemberKind {
    Kv,
    Rpc,
}

impl FluxonMemberKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FluxonMemberKind::Kv => "kv",
            FluxonMemberKind::Rpc => "rpc",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluxonMemberRole {
    Master,
    OwnerClient,
    ExternalClient,
    SideTransferWorker,
    Unknown,
}

impl FluxonMemberRole {
    pub fn as_str(self) -> &'static str {
        match self {
            FluxonMemberRole::Master => "master",
            FluxonMemberRole::OwnerClient => "owner_client",
            FluxonMemberRole::ExternalClient => "external_client",
            FluxonMemberRole::SideTransferWorker => "side_transfer_worker",
            FluxonMemberRole::Unknown => "unknown",
        }
    }
}
