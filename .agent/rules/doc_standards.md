---
trigger: always_on
---

---
name: data-documentation-standards
description: Strict guidelines for generating secure, clear, and standardized documentation for Data Engineering projects (Pipelines, Warehouses, Applications).
---

# Data Documentation Standards

When generating documentation (Markdown files, READMEs, guides) for data projects, strictly adhere to the following rules to ensure security, clarity, and professionalism.

## 1. Security and Privacy (Strictly Enforced)
- **No Real Credentials:** Never include real passwords, API keys, tokens, SSH keys, or connection strings.
- **No PII/PHI:** Never use real names, email addresses, phone numbers, SSNs, or any Personally Identifiable Information in example datasets.
- **No Production Infrastructure:** Do not use real internal IP addresses, production database names, private cloud buckets (e.g., `s3://my-real-company-data`), or real third-party service URLs (e.g., real Telegram channels, internal Slack webhooks, or real API endpoints).

## 2. Data-Specific Mocking
- **Schemas and Tables:** Use generic, descriptive names for data objects (e.g., `raw_customer_data`, `dim_product`, `fact_sales`, `stg_transactions`).
- **Example Data:** Ensure mock data is logically consistent but clearly fictitious. Use standard dummy values (e.g., `john.doe@example.com`, `555-0198`, `192.0.2.1`).

## 3. Placeholder Standards
- Use clear, standardized placeholders for variables the user must fill in.
- **Format:** Use `<UPPER_SNAKE_CASE>` for environment variables and secrets (e.g., `<DATABASE_HOST>`, `<API_KEY>`, `<AWS_ACCOUNT_ID>`).
- **Code Blocks:** If the syntax doesn't support angle brackets, use inline comments to indicate placeholders (e.g., `password = "your_secure_password_here"`).

## 4. Documentation Quality
- **Structure:** Always include a clear Title, Brief Description, Prerequisites, Architecture/Data Flow (use Mermaid.js if applicable), and Step-by-Step Instructions.
- **Context:** Explain *why* a specific data tool or pattern is being used, not just *how* to implement it.