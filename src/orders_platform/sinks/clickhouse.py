from datetime import timezone


def insert_customers_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    customer_columns = [
        'customer_id',
        'email',
        'region',
        'segment',
        'created_at',
        'updated_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*customer_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in customer_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.customers_cdc',
                data=buffer,
                column_names=customer_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
                table='staging.customers_cdc',
            data=buffer,
            column_names=customer_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows


def insert_orders_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    order_columns = [
        'order_id',
        'customer_id',
        'status',
        'currency',
        'shipping_amount',
        'delivery_region',
        'created_at',
        'updated_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*order_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in order_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.orders_cdc',
                data=buffer,
                column_names=order_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
            table='staging.orders_cdc',
            data=buffer,
            column_names=order_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows


def insert_order_items_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    order_columns = [
        'order_item_id',
        'order_id',
        'product_id',
        'quantity',
        'unit_price',
        'discount_amount',
        'created_at',
        'updated_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*order_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in order_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.order_items_cdc',
                data=buffer,
                column_names=order_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
            table='staging.order_items_cdc',
            data=buffer,
            column_names=order_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows


def insert_payments_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    payment_columns = [
        'payment_id',
        'order_id',
        'amount',
        'currency',
        'status',
        'payment_method',
        'created_at',
        'updated_at',
        'succeeded_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*payment_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'succeeded_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in payment_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.payments_cdc',
                data=buffer,
                column_names=payment_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
            table='staging.payments_cdc',
            data=buffer,
            column_names=payment_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows


def insert_refunds_history(client, history_df, batch_size=1000):
    if batch_size <= 0:
        raise ValueError('Batch size cannot be 0')

    refund_columns = [
        'refund_id',
        'payment_id',
        'amount',
        'status',
        'created_at',
        'updated_at',
        'succeeded_at',
        'operation',
        'source_changed_at',
        'topic',
        'partition',
        'offset',
        'ingested_at'
    ]

    buffer = []
    inserted_rows = 0
    for row in history_df.select(*refund_columns).toLocalIterator():
        vals = row.asDict()
        for col in ['created_at', 'updated_at', 'succeeded_at', 'source_changed_at', 'ingested_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        buffer.append(tuple(vals[col] for col in refund_columns))

        if len(buffer) >= batch_size:
            client.insert(
                table='staging.refunds_cdc',
                data=buffer,
                column_names=refund_columns
            )
            inserted_rows += len(buffer)
            buffer.clear()

    if buffer:
        client.insert(
            table='staging.refunds_cdc',
            data=buffer,
            column_names=refund_columns
        )
        inserted_rows += len(buffer)
    return inserted_rows