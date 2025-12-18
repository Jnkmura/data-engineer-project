connection: "gcp"

include: "/views/**/*.view.lkml"

datagroup: models_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: models_default_datagroup

explore: transactions {
  label: "Transações de vendas"
  description: "Explore com transações de vendas de diversas regiões para ser usado pelo time de negócios"
  group_label: "Vendas"
  from: sales_transactions 
}