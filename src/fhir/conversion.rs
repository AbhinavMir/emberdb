use super::FHIRError;
use crate::storage::Record;

pub trait FHIRConverter {
    fn to_records(&self) -> Vec<Record>;
    fn from_records(records: &[Record]) -> Result<Self, FHIRError> 
    where
        Self: Sized;
} 