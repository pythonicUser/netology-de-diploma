CREATE TABLE IF NOT EXISTS invoices_fct
(
        invoice_id String, 
		branch_name String, 
		city_name String,
		customer_type String,
		payment_type String,
		product_line String,
		gender String,
		invoice_date DATETIME,
		rating Float32,
		quantity UInt32,
		unit_price Float64,
		cogs Float64,
		tax_5_perc Float64,
		total Float64,
		gross_income Float64,
		gross_income_perc Float64,
		created_at_nds TIMESTAMP
        
) ENGINE = MergeTree()
  PARTITION BY (toYYYYMM(invoice_date), branch_name)
  ORDER BY invoice_date;