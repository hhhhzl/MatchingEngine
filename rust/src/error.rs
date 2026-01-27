//! Error types for the matching engine

use thiserror::Error;

/// Result type alias for matching engine operations
pub type Result<T> = std::result::Result<T, MatchingError>;

/// Errors that can occur during order matching operations
#[derive(Error, Debug, Clone, PartialEq)]
pub enum MatchingError {
    /// Order not found in the order book
    #[error("Order not found: {0}")]
    OrderNotFound(String),
    
    /// Invalid order parameters
    #[error("Invalid order: {0}")]
    InvalidOrder(String),
    
    /// Order already exists in the order book
    #[error("Order already exists: {0}")]
    OrderExists(String),
    
    /// Order book not found for the given symbol
    #[error("Orderbook not found for symbol: {0}")]
    OrderbookNotFound(String),
    
    /// Cannot cancel order in current state
    #[error("Cannot cancel order in state: {0:?}")]
    InvalidCancelState(String),
    
    /// Price must be positive for limit orders
    #[error("Price must be positive for limit orders")]
    InvalidPrice,
    
    /// Quantity must be positive
    #[error("Quantity must be positive")]
    InvalidQuantity,
    
    /// Order type not supported
    #[error("Order type not supported: {0:?}")]
    UnsupportedOrderType(String),
    
    /// Time in force not supported
    #[error("Time in force not supported: {0:?}")]
    UnsupportedTimeInForce(String),
}
