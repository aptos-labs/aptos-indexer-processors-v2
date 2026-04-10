use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};

#[allow(clippy::too_long_first_doc_paragraph)]
/// This struct holds additional headers that we attach to the request metadata.
/// Regarding serde, we just serialize this as we would a HashMap<String, String>.
/// Similarly, we expect that format when deserializing.
///
/// It is necessary to use HashMap because there is no extend method on MetadataMap
/// itself, nor does it implement Serialize / Deserialize. It is better to parse once
/// here right at config validation time anyway, it exposes any error as early as
/// possible and saves us doing parsing (perhaps multiple times) later.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(try_from = "HashMap<String, String>")]
#[serde(into = "HashMap<String, String>")]
pub struct AdditionalHeaders(HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>);

impl AdditionalHeaders {
    pub fn drain_into_metadata_map(self, metadata_map: &mut MetadataMap) {
        for (key, value) in self.0 {
            metadata_map.insert(key, value);
        }
    }
}

impl TryFrom<HashMap<String, String>> for AdditionalHeaders {
    type Error = anyhow::Error;

    /// Build `AdditionalHeaders` from just a map of strings. This can fail if the
    /// strings contain invalid characters for metadata keys / values, the chars must
    /// only be visible ascii characters.
    fn try_from(map: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut out = HashMap::new();
        for (k, v) in map {
            let k = MetadataKey::from_str(&k)
                .with_context(|| format!("Failed to parse key as ascii metadata key: {k}"))?;
            let v = MetadataValue::from_str(&v)
                .with_context(|| format!("Failed to parse value as ascii metadata value: {v}"))?;
            out.insert(k, v);
        }
        Ok(AdditionalHeaders(out))
    }
}

impl From<AdditionalHeaders> for HashMap<String, String> {
    fn from(headers: AdditionalHeaders) -> Self {
        headers
            .0
            .into_iter()
            // It is safe to unwrap here because when building this we asserted that the
            // MetadataValue only contained visible ascii characters.
            .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap().to_owned()))
            .collect()
    }
}
