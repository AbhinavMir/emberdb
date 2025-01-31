use std::sync::Arc;
use warp::Filter;
use crate::timeseries::query::QueryEngine;

pub struct RestApi {
    query_engine: Arc<QueryEngine>,
}

impl RestApi {
    pub fn new(query_engine: Arc<QueryEngine>) -> Self {
        RestApi { query_engine }
    }

    pub fn routes(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // Basic CRUD endpoints
        self.get_observation()
            .or(self.post_observation())
            .or(self.get_patient())
    }

    fn get_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        // Basic implementation - expand this later
        warp::path!("fhir" / "Observation")
            .and(warp::get())
            .map(|| warp::reply::json(&"Not implemented yet"))
    }

    fn post_observation(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fhir" / "Observation")
            .and(warp::post())
            .map(|| warp::reply::json(&"Not implemented yet"))
    }

    fn get_patient(&self) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("fhir" / "Patient")
            .and(warp::get())
            .map(|| warp::reply::json(&"Not implemented yet"))
    }
} 