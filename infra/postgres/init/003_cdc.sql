create role debezium
    LOGIN
    REPLICATION
    NOSUPERUSER;

grant connect 
on database shop 
to debezium;

grant usage 
on schema public 
to debezium;

grant select 
on table 
    public.customers,
    public.products,
    public.orders,
    public.order_items,
    public.payments,
    public.refunds
to debezium;

create publication 
for table
    public.customers,
    public.products,
    public.orders,
    public.order_items,
    public.payments,
    public.refunds;