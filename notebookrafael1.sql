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

-- COMMAND ----------

SELECT *,

--- ISSO EH UMA COLUNA NOVA
       CASE WHEN descuf = 'SP' THEN 'PAULISTA' 
            WHEN descuf = 'RJ' THEN 'FLUMINENSE' 
            WHEN descuf = 'PR' THEN 'PARANAENSE' 
       
       ELSE 'OUTROS'
        
        END AS DESCNACIONALIDADE,
        
  -- ISSO EH OUTRA COLUNA NOVA  COM LIKE   e pode colocar o NOT LIKE para dizer o contrario
        
        CASE WHEN DESCCIDADE LIKE '%sao%' THEN 'tem sao no nome'
        
         ELSE 'NAO TEM SAO'
         
         END AS DESCCIDADESAO
       
FROM silver_olist.CLIENTE

-- COMMAND ----------

SELECT *,

       CASE WHEN descuf IN ('SP', 'MG' ,'RJ', 'ES')  THEN 'SUDESTE'
             
      ELSE 'OUTROS'
      
      END AS DESCREGIAO
      
       
  FROM silver_olist.CLIENTE

-- COMMAND ----------

-- SELECAO CLIENTES PAULISTANOS

SELECT *
           
  FROM silver_olist.CLIENTE
  
     WHERE DESCCIDADE = 'sao paulo'

-- COMMAND ----------

-- SELECAO CLIENTES PAULISTAS

SELECT *
           
  FROM silver_olist.CLIENTE
  
     WHERE DESCUF = 'SP'
