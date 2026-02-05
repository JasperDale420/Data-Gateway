# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.5.x   | ✅        |
| < 0.5   | ❌        |

## Reporting a Vulnerability

For security vulnerabilities, please contact the maintainers directly rather than opening a public issue.

## Security Features

### Authentication

- **API Key Authentication**: All REST and WebSocket endpoints require authentication
- **Key Format**: `gw_<client_id>_<random>` with 256-bit entropy
- **Key Storage**: SHA-256 hashed, never stored in plaintext
- **Timeout**: Authentication must complete within 10 seconds
- **Roles**: Admin endpoints require a client role of `admin` or `super_admin`
- **Replay WebSocket**: `X-Gateway-Key` header is required during the handshake
- **Trading**: Alpaca account/trading endpoints require a client role of `trader`, `admin`, or `super_admin`

### Rate Limiting

| Level | Limit |
|-------|-------|
| Global | 10,000 requests/min |
| Per-IP | 1,000 requests/min |
| Per-Client | 600 requests/min (configurable) |

### Authorization

- Provider access is enforced via `permissions.providers` in `config/clients.yaml`.
- Feed access is enforced via `permissions.feeds`.
- Per-request symbol limits are enforced via `permissions.max_symbols`.
- WebSocket subscription limits are enforced via `permissions.ws_subscriptions_max`.
- Bulk jobs and replay sessions are scoped to the client that created them.

### Transport Security

- TLS 1.3 recommended for production
- HSTS headers enabled via `SecurityHeadersMiddleware`
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY

### Input Validation

- Symbol pattern validation (rejects invalid characters)
- Parameter limits (max symbols, date ranges)
- Request size limits
- Forbidden character rejection

### DDoS Protection

- Maximum 10 connections per IP
- Auth failure tracking and blocking
- IP blocklist management via admin endpoints

## Configuration Best Practices

### Production Deployment

1. **Never use debug mode in production**

   ```shell
   GATEWAY_DEBUG=false
   ```

2. **Use hashed API keys**
   - Generate keys with: `python -m gateway.cli generate-key`
   - Store `key_hash` in `config/clients.yaml`, not plaintext

3. **Set appropriate rate limits**

   ```yaml
   permissions:
     rate_limit: 600  # requests per minute
   ```

4. **Enable TLS termination** at load balancer or reverse proxy

5. **Restrict CORS origins**

   ```shell
   # In production, GATEWAY_DEBUG=false restricts CORS
   ```

### Secrets Management

- Use environment variables or Docker secrets
- Never commit API keys to version control
- Rotate provider API keys periodically

## Security Headers

The `SecurityHeadersMiddleware` adds:

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
X-XSS-Protection: 1; mode=block
Referrer-Policy: strict-origin-when-cross-origin
```

## Audit Logging

Security events are logged in structured JSON format:

- `auth_success` - Successful authentication
- `auth_failed` - Failed authentication attempt
- `rate_limit_exceeded` - Rate limit triggered
- `ip_blocked` - IP address blocked

## Dependencies

Security scanning is performed by:

- **bandit** - Python security linter
- **detect-secrets** - Prevents accidental secret commits
- **SonarCloud** - Continuous code analysis
