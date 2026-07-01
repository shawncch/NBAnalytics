

select 'user1' as user_id, 'id1' as transaction_id, 'visa' as payment_method, 27 as payment_amount
union all
select 'user2' as user_id, 'id2' as transaction_id, 'visa' as payment_method, 54 as payment_amount
union all
select 'user3' as user_id, 'id3' as transaction_id, 'paypal' as payment_method, 12 as payment_amount
union all
select 'user1' as user_id, 'id4' as transaction_id, 'paypal' as payment_method, 99 as payment_amount
union all
select 'user4' as user_id, 'id5' as transaction_id, 'mastercard' as payment_method, 45 as payment_amount
union all
select 'user2' as user_id, 'id6' as transaction_id, 'mastercard' as payment_method, 18 as payment_amount
