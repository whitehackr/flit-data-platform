{{ config(materialized='view') }}

{#
  BNPL Customer Tenure Adjustment - Realistic Tenure Over Time

  Problem: customer_tenure_days is static across all transactions (e.g., always 261 days)
  Solution: Create realistic tenure that grows with each transaction

  Business Logic:
  1. Capture customer's tenure at first transaction (baseline)
  2. Calculate days since first transaction for each subsequent transaction
  3. Adjust tenure = baseline + days_elapsed for realistic growth over time
#}

with bnpl_staged as (
    select * ,
    to_hex(md5(concat(
            customer_age_bracket, customer_credit_score_range, customer_id, customer_income_bracket, customer_state
        ))) as unique_customer_id,
    from {{ ref('stg_bnpl_raw_transactions') }}
),

customer_first_transaction as (
    select
        customer_id,
        min(transaction_timestamp) as first_customer_transaction_date,
        -- Get the customer_tenure_days from their very first transaction
        min_by(customer_tenure_days, transaction_timestamp) as first_customer_tenure
    from bnpl_staged
    where customer_tenure_days is not null
    group by customer_id
),

transactions_with_adjusted_tenure as (
    select
        bs.*,

        -- Add customer's first transaction context
        cft.first_customer_transaction_date,
        cft.first_customer_tenure,

        -- Calculate days elapsed since first transaction
        date_diff(
            date(bs.transaction_timestamp),
            date(cft.first_customer_transaction_date),
            day
        ) as days_from_first_transaction,

        -- Create realistic growing tenure
        cft.first_customer_tenure + date_diff(
            date(bs.transaction_timestamp),
            date(cft.first_customer_transaction_date),
            day
        ) as adjusted_customer_tenure

    from bnpl_staged bs
    inner join customer_first_transaction cft
        on bs.customer_id = cft.customer_id
)

select * from transactions_with_adjusted_tenure