//! Shared live-service fixtures for the MongoDB tests.

use crate::common::{Seed, seed};
use chrono::{DateTime, TimeZone, Utc};
use siderite_backends::mongodb::{Keys, MongoBackend};
use siderite_orm::{BackendCapabilityError, Db, Feature, OrmError};
use uuid::Uuid;

pub struct TestDb {
    pub db: Db,
    pub backend: MongoBackend,
}

impl TestDb {
    pub async fn open() -> Option<Self> {
        let Some(url) = std::env::var("MONGODB_URL")
            .ok()
            .filter(|u| u.starts_with("mongodb"))
        else {
            panic!("live mongodb tests require MONGODB_URL");
        };
        let name = format!("siderite_test_{}", Uuid::new_v4().simple());
        let backend = connect(&url, &name)
            .await
            .with_keys(Keys::default().with("tags", "slug"));
        Some(Self {
            db: Db::new(backend.clone()),
            backend,
        })
    }

    /// A fresh database whose backend reserves generated keys `size` at a
    /// time.
    pub async fn with_key_block_size(size: u32) -> Option<Self> {
        let t = Self::open().await?;
        let backend = t.backend.with_key_block_size(size);
        Some(Self {
            db: Db::new(backend.clone()),
            backend,
        })
    }

    /// The `seq` of the `siderite_counters` document of `collection`.
    pub async fn counter(&self, collection: &str) -> Option<i64> {
        self.backend
            .database()
            .collection::<mongodb::bson::Document>("siderite_counters")
            .find_one(mongodb::bson::doc! { "_id": collection })
            .await
            .unwrap()
            .map(|d| d.get_i64("seq").unwrap())
    }

    pub async fn seeded() -> Option<(Self, Seed)> {
        let t = Self::open().await?;
        let seed = seed(&t.db).await;
        Some((t, seed))
    }

    pub async fn cleanup(self) {
        self.backend.database().drop().await.unwrap();
    }
}

/// `SIDERITE_MONGO_NATIVE=1` runs the suite with the native path
/// forced on (feature `mongodb-native`).
pub fn native() -> bool {
    std::env::var("SIDERITE_MONGO_NATIVE").is_ok_and(|v| v == "1")
}

/// The backend under test: native when [`native`] asks for it (failing
/// loudly when the deployment does not qualify), the driver otherwise.
pub async fn connect(url: &str, name: &str) -> MongoBackend {
    if native() {
        #[cfg(feature = "mongodb-native")]
        {
            use siderite_backends::mongodb::DataPath;
            let connect = MongoBackend::connect_native(url, name);
            let backend = connect.await.unwrap();
            assert_eq!(
                backend.data_path(),
                DataPath::Native,
                "native path requested but ineligible: {:?}",
                backend.native_ineligibility()
            );
            return backend;
        }
        #[cfg(not(feature = "mongodb-native"))]
        panic!("SIDERITE_MONGO_NATIVE=1 needs the mongodb-native feature");
    }
    MongoBackend::connect(url, name).await.unwrap()
}

pub fn is_capability(result: &Result<impl std::fmt::Debug, OrmError>, wanted: Feature) -> bool {
    matches!(
        result,
        Err(OrmError::Capability(BackendCapabilityError::Unsupported { feature, .. }))
            if *feature == wanted
    ) || matches!(
        (result, wanted),
        (
            Err(OrmError::Capability(
                BackendCapabilityError::RowLockingUnsupported { .. }
            )),
            Feature::RowLocking
        )
    )
}

pub fn ms(t: DateTime<Utc>) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(t.timestamp_millis()).unwrap()
}
