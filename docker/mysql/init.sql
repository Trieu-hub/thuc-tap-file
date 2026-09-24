-- Runs once, on the first start of an empty MySQL volume.
-- One schema + one user per service (CLAUDE.md D10). Passwords are demo-only.

CREATE DATABASE IF NOT EXISTS order_db   CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS payment_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS policy_db  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'order_svc'@'%'   IDENTIFIED BY 'order_demo_pwd';
CREATE USER IF NOT EXISTS 'payment_svc'@'%' IDENTIFIED BY 'payment_demo_pwd';
CREATE USER IF NOT EXISTS 'policy_svc'@'%'  IDENTIFIED BY 'policy_demo_pwd';

-- Each user can only touch its own schema: a service reaching into another service's DB fails fast.
GRANT ALL PRIVILEGES ON order_db.*   TO 'order_svc'@'%';
GRANT ALL PRIVILEGES ON payment_db.* TO 'payment_svc'@'%';
GRANT ALL PRIVILEGES ON policy_db.*  TO 'policy_svc'@'%';
FLUSH PRIVILEGES;
