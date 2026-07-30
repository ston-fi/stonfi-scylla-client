#[derive(Clone, Copy, Debug, strum::IntoStaticStr, PartialEq, Eq)]
pub(crate) enum QueryType {
    Select,
    Insert,
    Delete,
    Execute,
}
