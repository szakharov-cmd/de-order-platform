create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
    NEW.updated_at = statement_timestamp();
    return NEW;
end;
$$;

create or replace trigger customers_set_updated_at
before update on customers
for each row 
execute function set_updated_at();

create or replace trigger products_set_updated_at
before update on products
for each row 
execute function set_updated_at();

create or replace trigger orders_set_updated_at
before update on orders
for each row 
execute function set_updated_at();

create or replace trigger order_items_set_updated_at
before update on order_items
for each row 
execute function set_updated_at();

create or replace trigger payments_set_updated_at
before update on payments
for each row
execute function set_updated_at();

create or replace trigger refunds_set_updated_at
before update on refunds
for each row 
execute function set_updated_at();