{% macro init_s3_sources() -%}

    {% set sources = [
        'DROP TABLE IF EXISTS src_v_sales'   
         ,  'CREATE TABLE IF NOT EXISTS src_v_sales
        (
        program_id 		Int64,
        user_id	 		Int64,
        money_received 	Int64,
        amount_rub		Decimal(15,2),
        cashback_rub	Decimal(15,2),
        commission_rub 	Decimal(15,2),
        sale_id			Int64,
        sale_date		DateTime,
        review_date 	DateTime
        )
        ENGINE = S3(\'https://storage.yandexcloud.net/discount-files2023/v_sales.csv\', \'CustomSeparated\')
        SETTINGS
             format_custom_field_delimiter=\'|\'
            ,format_custom_escaping_rule=\'CSV\'
            ,format_custom_row_after_delimiter=\'|\n\' 
        '
    ] %}

    {% for src in sources %}
        {% set statement = run_query(src) %}
    {% endfor %}

{{ print('Initialized source tables (S3)') }}    

{%- endmacro %}