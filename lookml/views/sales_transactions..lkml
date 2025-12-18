view: sales_transactions {
  sql_table_name: raw.sales_transactions ;;

  dimension: sk {
    type: string
    primary_key: yes
    sql: ${TABLE}.sk
    hidden: yes
  }

  dimension: order_id {
    type: string
    sql: ${TABLE}.order_id
    hidden: yes
  }

  dimension: item_id {
    type: string
    sql: ${TABLE}.item_id
    hidden: yes
  }

  dimension: item_value {
    type: string
    sql: ${TABLE}.item_value
    hidden: yes
  }

  dimension: status {
    label: "Status da transação de venda"
    description: "Status da transação de venda (Completed / In progress)"
    type: string
    sql: ${TABLE}.status
  }

  dimension: transaction_date {
    label: "Data de transação"
    description: "Data de transação de vendas"
    type: date
    sql: date(${TABLE}.timestamp) ;;
  }

  dimension_group: date {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.timestamp ;;
  }

  measure: total_revenue {
    label: "Receita total"
    description: "Valor total vendido (quaisquer status)"
    type: sum
    sql: ${item_value}  ;;
  }

  measure: unique_orders {
    label: "Pedidos"
    description: "Quantidade de pedidos vendidos (quaisquer status)"
    type: sum
    sql: ${item_value}  ;;
  }

  measure: average_order_value {
    label: "Valor médio por pedido"
    description: "Valor médio por pedido (quaisquer status)"
    type: number
    sql: safe_divide(${total_revenue}, ${unique_orders}) ;;
  }
}