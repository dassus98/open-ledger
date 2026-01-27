select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select settlement_id
from OPENLEDGER.ANALYTICS.staging_settlements
where settlement_id is null



      
    ) dbt_internal_test