-- Singular test: returns inconsistent orders; an empty result means success.
-- NULL values are covered by the model's separate not_null tests.
select *
from {{ ref('orders_obt') }}
where order_total_amount != items_net_amount + shipping_amount
   or items_net_amount != items_gross_amount - discount_amount
   or net_collected_amount != paid_amount - refunded_amount
   or is_fully_paid != if(
       successful_payment_count > 0 and paid_amount >= order_total_amount,
       1,
       0
   )
   or items_gross_amount < 0
   or discount_amount < 0
   or items_net_amount < 0
   or shipping_amount < 0
   or order_total_amount < 0
   or paid_amount < 0
   or refunded_amount < 0
   or net_collected_amount < 0
   or line_count < 0
   or item_quantity < 0
   or payment_attempt_count < 0
   or successful_payment_count < 0
   or failed_payment_count < 0
   or refund_attempt_count < 0
   or successful_refund_count < 0
   or failed_refund_count < 0
