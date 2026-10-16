#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bridging L2–L3MemberKind {
    Kv,
    Rpc,
}

impl Bridging L2–L3MemberKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Bridging L2–L3MemberKind::Kv => "kv",
            Bridging L2–L3MemberKind::Rpc => "rpc",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bridging L2–L3MemberRole {
    Master,
    OwnerClient,
    ExternalClient,
    SideTransferWorker,
    Unknown,
}

impl Bridging L2–L3MemberRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Bridging L2–L3MemberRole::Master => "master",
            Bridging L2–L3MemberRole::OwnerClient => "owner_client",
            Bridging L2–L3MemberRole::ExternalClient => "external_client",
            Bridging L2–L3MemberRole::SideTransferWorker => "side_transfer_worker",
            Bridging L2–L3MemberRole::Unknown => "unknown",
        }
    }
}
