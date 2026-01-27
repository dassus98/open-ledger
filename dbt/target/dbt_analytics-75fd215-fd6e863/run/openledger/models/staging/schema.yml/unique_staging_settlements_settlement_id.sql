select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    settlement_id as unique_field,
    count(*) as n_records

from OPENLEDGER.ANALYTICS.staging_settlements
where settlement_id is not null
group by settlement_id
having count(*) > 1



      
    ) dbt_internal_test