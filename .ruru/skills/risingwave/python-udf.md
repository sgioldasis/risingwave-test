# RisingWave Embedded Python UDF

Create and use embedded Python user-defined functions in RisingWave for custom data transformations.

## Prerequisites

Enable Python UDFs in [`risingwave.toml`](risingwave.toml:377):
```toml
[udf]
enable_embedded_python_udf = true
```

Restart RisingWave after configuration change.

## Usage

```sql
CREATE FUNCTION <function_name>(<param1> <type1>, ...) 
RETURNS <return_type> 
LANGUAGE python AS $$
    <python_code>
$$;
```

## Supported Data Types

| RisingWave Type | Python Type |
|-----------------|-------------|
| `BOOLEAN` | `bool` |
| `SMALLINT`, `INT`, `BIGINT` | `int` |
| `REAL`, `DOUBLE`, `DECIMAL` | `float` |
| `VARCHAR` | `str` |
| `BYTEA` | `bytes` |
| `DATE`, `TIME`, `TIMESTAMP` | `datetime` |
| `ARRAY<T>` | `list` |
| `STRUCT<...>` | `dict` |

## Examples

### Conversion Rate Categorization
```sql
CREATE FUNCTION conversion_category(rate float) 
RETURNS varchar 
LANGUAGE python AS $$
    if rate >= 0.5:
        return "excellent"
    elif rate >= 0.3:
        return "good"
    elif rate >= 0.1:
        return "average"
    else:
        return "needs_improvement"
$$;
```

### Revenue Tier Classification
```sql
CREATE FUNCTION revenue_tier(amount numeric) 
RETURNS varchar 
LANGUAGE python AS $$
    if amount >= 1000:
        return "premium"
    elif amount >= 500:
        return "high"
    elif amount >= 100:
        return "medium"
    else:
        return "low"
$$;
```

### Complex Business Logic
```sql
CREATE FUNCTION calculate_funnel_score(
    viewers bigint,
    carters bigint,
    purchasers bigint
) RETURNS float 
LANGUAGE python AS $$
    if viewers == 0:
        return 0.0
    
    view_to_cart = carters / viewers
    cart_to_buy = purchasers / carters if carters > 0 else 0
    
    # Weighted score
    return view_to_cart * 0.4 + cart_to_buy * 0.6
$$;
```

### Array Processing
```sql
CREATE FUNCTION format_metrics(metrics float[]) 
RETURNS varchar 
LANGUAGE python AS $$
    labels = ['viewers', 'carters', 'purchasers']
    parts = [f"{label}={value:.2f}" for label, value in zip(labels, metrics)]
    return ", ".join(parts)
$$;
```

## Using UDFs in Materialized Views

```sql
CREATE MATERIALIZED VIEW funnel_enriched AS
SELECT 
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    conversion_category(view_to_cart_rate) as view_to_cart_category,
    conversion_category(cart_to_buy_rate) as cart_to_buy_category,
    calculate_funnel_score(viewers, carters, purchasers) as funnel_score
FROM funnel;
```

## Best Practices

1. **Keep functions simple** - Complex logic can impact performance
2. **Handle nulls** - Always check for None in Python
3. **Type safety** - Match RisingWave types to Python types carefully
4. **Error handling** - Return default values instead of raising exceptions

## Management Commands

```sql
-- List all functions
SHOW FUNCTIONS;

-- Drop a function
DROP FUNCTION conversion_category;

-- Check function definition
SHOW CREATE FUNCTION conversion_category;
```

## Integration with dbt

Create a macro for UDFs:

```sql
-- macros/create_udfs.sql
{% macro create_udfs() %}

CREATE FUNCTION IF NOT EXISTS conversion_category(rate float) 
RETURNS varchar 
LANGUAGE python AS $$
    if rate >= 0.5:
        return "excellent"
    elif rate >= 0.3:
        return "good"
    elif rate >= 0.1:
        return "average"
    else:
        return "needs_improvement"
$$;

{% endmacro %}
```

Run in dbt model:
```sql
-- models/setup_udfs.sql
{{ config(materialized='ephemeral') }}

{{ create_udfs() }}
```

## Related Skills

- `risingwave/materialized-view` - Use UDFs in views
- `risingwave/kafka-source` - Transform streaming data with UDFs
