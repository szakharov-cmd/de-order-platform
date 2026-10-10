create table if not exists customers (
    customer_id bigint generated always as identity primary key,
    email text,
    region text,
    segment text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists products (
    product_id bigint generated always as identity primary key,
    name text not null,
    category text not null,
    brand text not null,
    current_price numeric(18,2) not null
        check (current_price >= 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists orders (
    order_id bigint generated always as identity primary key,
    customer_id bigint not null,
        constraint fk_order_customers
            foreign key (customer_id)
            references customers (customer_id),
    status text not null
        check (status in ('created', 'paid', 'shipped', 'delivered', 'cancelled')),
    currency varchar(3) not null,
    shipping_amount numeric(18, 2) not null
        check (shipping_amount >= 0),
    delivery_region text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_order_customer_id
    on orders (customer_id);

create table if not exists order_items (
    order_item_id bigint generated always as identity primary key,
    order_id bigint not null,
        constraint fk_item_order
            foreign key (order_id)
            references orders (order_id),
    product_id bigint not null,
        constraint fk_item_product
            foreign key (product_id)
            references products (product_id),
    quantity integer not null
        check (quantity > 0),
    unit_price numeric(18, 2) not null
        check (unit_price >= 0),
    discount_amount numeric(18, 2) not null default 0
        check (discount_amount >= 0 and discount_amount <= quantity * unit_price),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_item_order_id 
    on order_items (order_id);
create index if not exists idx_item_product_id 
    on order_items (product_id);

create table if not exists payments (
    payment_id bigint generated always as identity primary key,
    order_id bigint not null,
        constraint fk_payment_order
            foreign key (order_id) 
            references orders (order_id),
    amount numeric(18, 2) not null
        check (amount > 0),
    currency varchar(3) not null,
    status text not null
        check (status in ('pending', 'succeeded', 'failed')),
    payment_method text not null
        check (payment_method in ('card', 'bank_transfer')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    succeeded_at timestamptz
        check (
            (status = 'succeeded' and succeeded_at is not null)
            or (status != 'succeeded' and succeeded_at is null)
        )
);
create index if not exists idx_payment_order_id
    on payments (order_id);

create table if not exists refunds (
    refund_id bigint generated always as identity primary key,
    payment_id bigint not null,
        constraint fk_refund_payment
            foreign key (payment_id)
            references payments (payment_id),
    amount numeric(18, 2) not null
        check (amount > 0),
    status text not null
        check (status in ('pending', 'succeeded', 'failed')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    succeeded_at timestamptz
        check (
            (status = 'succeeded' and succeeded_at is not null)
            or (status != 'succeeded' and succeeded_at is null)
        )
);
create index if not exists idx_refund_payment_id
    on refunds (payment_id);