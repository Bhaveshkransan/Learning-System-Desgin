
CREATE TABLE IF NOT EXISTS circuit_breakers (
    service_name VARCHAR(100) PRIMARY KEY,
    state VARCHAR(20) NOT NULL DEFAULT 'CLOSED',
    failure_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO circuit_breakers (
    service_name,
    state,
    failure_count
)
VALUES (
    'profile-service',
    'CLOSED',
    0
)
ON CONFLICT (service_name) DO NOTHING;

