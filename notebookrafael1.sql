-- Databricks notebook source
SELECT * FROM silver_olist.pedido

-- COMMAND ----------

SELECT * FROM silver_olist.pedido

where descsituacao = 'canceled'

-- COMMAND ----------

SELECT * 

FROM silver_olist.pedido

where descsituacao = 'canceled'
and year(dtpedido) = '2018'

-- COMMAND ----------

SELECT * 

FROM silver_olist.pedido

where (descsituacao = 'canceled' or descsituacao = 'shiped' )

and year(dtpedido) = '2018'

-- COMMAND ----------

SELECT * 

FROM silver_olist.pedido

where descsituacao IN ('canceled','shipped')

and year(dtpedido) = '2018'

-- COMMAND ----------

SELECT * 

FROM silver_olist.pedido

where descsituacao IN ('canceled','shipped')

and year(dtpedido) = '2018'
and datediff (dtestimativaentrega,dtaprovado) > 30

-- COMMAND ----------

SELECT *,
       datediff (dtestimativaentrega,dtaprovado)  as di

FROM silver_olist.pedido

where descsituacao IN ('canceled','shipped')

and year(dtpedido) = '2018'
and datediff (dtestimativaentrega,dtaprovado) > 30

-- COMMAND ----------

SELECT *,
       CASE WHEN descuf = 'SP' THEN 'PAULISTA'  END AS DESCNACIONALIDADE
FROM silver_olist.CLIENTE


-- COMMAND ----------

SELECT *,
       CASE WHEN descuf = 'SP' THEN 'PAULISTA' 
       ELSE 'OUTROS'
        END AS DESCNACIONALIDADE
       
FROM silver_olist.CLIENTE

-- COMMAND ----------

SELECT *,
       CASE WHEN descuf = 'SP' THEN 'PAULISTA' 
            WHEN descuf = 'RJ' THEN 'FLUMINENSE' 
       
       ELSE 'OUTROS'
        END AS DESCNACIONALIDADE
       
FROM silver_olist.CLIENTE

-- COMMAND ----------

SELECT *,
       CASE WHEN descuf = 'SP' THEN 'PAULISTA' 
            WHEN descuf = 'RJ' THEN 'FLUMINENSE' 
            WHEN descuf = 'PR' THEN 'PARANAENSE' 
       
       ELSE 'OUTROS'
        
        END AS DESCNACIONALIDADE
       
FROM silver_olist.CLIENTE
