from datetime import timezone

def insert_customers_state(client, state_df):
    customer_columns = [
        'customer_id',
        'email',
        'region',
        'segment',
        'created_at',
        'updated_at',
        'source_partition',
        'source_offset',
        'is_deleted'
    ]
    rows = state_df.collect()
    data = []
    for row in rows:
        vals = row.asDict()
        for col in ['created_at', 'updated_at']:
            dt = vals.get(col)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
            vals[col] = dt
        data.append(tuple(vals[col] for col in customer_columns))

    if data:
        client.insert(
            table='silver_state.customers',
            data=data,
            column_names=customer_columns
        )
        print(f'Inserted {len(data)} rows')
    else:
        print(f'No rows inserted')