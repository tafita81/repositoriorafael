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

-- COMMAND ----------

-- SELECAO VENDEDORES 

SELECT *
           
  FROM silver_olist.VENDEDOR
  


-- COMMAND ----------

-- SELECAO VENDEDORES CARIOCAS E PAULISTAS

SELECT *
           
  FROM silver_olist.VENDEDOR
  
  WHERE DESCCIDADE ='rio de janeiro'
  
  OR DESCUF = 'SP'

-- COMMAND ----------

-- SELECAO PRODUTOS BEBES E PERFUMARIA

SELECT *
           
  FROM silver_olist.PRODUTO
  


-- COMMAND ----------

-- SELECAO PRODUTOS BEBES E PERFUMARIA E ALTURA MAIO 5 CM

SELECT *
           
  FROM silver_olist.PRODUTO
  
WHERE DESCCATEGORIA IN ('perfuramia' , 'bebes')

AND VLALTURACM > 5

-- COMMAND ----------

-- AGREGAR DADOS -CONTAR CLIENTES (LINHAS NAO NULAS)

SELECT COUNT(*)  AS NUMEROLINHASNAONULAS

           
  FROM silver_olist.CLIENTE
  


-- COMMAND ----------


SELECT COUNT(*)  AS NUMEROLINHASNAONULAS,
      COUNT (IDCLIENTE)  AS CLIENTESNAONULOS,
      COUNT (DISTINCT IDCLIENTE)  AS IDCLIENTESNAONULOS,
        COUNT (DISTINCT IDCLIENTEUNICO)  AS NUMEROCLIENTEUNICODISTINTO
           
  FROM silver_olist.CLIENTE

-- COMMAND ----------


SELECT *
           
  FROM silver_olist.CLIENTE

-- COMMAND ----------


SELECT COUNT(*)  AS NUMEROLINHASNAONULAS,
      COUNT (IDCLIENTE)  AS CLIENTESNAONULOS,
      COUNT (DISTINCT IDCLIENTE)  AS IDCLIENTESNAONULOS,
        COUNT (DISTINCT IDCLIENTEUNICO)  AS NUMEROCLIENTEUNICODISTINTO
           
  FROM silver_olist.CLIENTE

-- COMMAND ----------

-- contar clientes ESTADO sao paulo

SELECT *
           
  FROM silver_olist.CLIENTE



-- COMMAND ----------

-- contar clientes ESTADO sao paulo

SELECT *
           
  FROM silver_olist.CLIENTE
  
  where DESCCIDADE = 'presidente prudente'

-- COMMAND ----------

-- contar clientes CIDADE PRESIDENTE PRUDENTE

SELECT count(*)
           
  FROM silver_olist.CLIENTE
  
  where DESCCIDADE = 'presidente prudente'

-- COMMAND ----------

-- contar clientes CIDADE PRESIDENTE PRUDENTE E CURITIBA

SELECT count(*) AS QTDLINHAS,
       count (distinct idcliente) as qtdclientes,
        count (distinct idclienteunico) as qtdclientesunicos
           
  FROM silver_olist.CLIENTE
  
  where DESCCIDADE IN ('presidente prudente','curitiba')

-- COMMAND ----------

SELECT *
           
  FROM silver_olist.item_pedido
  


-- COMMAND ----------

SELECT 
  
  avg (vlpreco)  AS AVGPRECO,
  avg (vlfrete)   AS AVGFRETE
           
  FROM silver_olist.item_pedido

-- COMMAND ----------

SELECT 
  
  avg (vlpreco)  AS AVGPRECO,
    MAX (vlpreco)  AS MAXPRECO,
  avg (vlfrete)   AS AVGFRETE,
    MAX (vlfrete)   AS MAXFRETE
           
  FROM silver_olist.item_pedido

-- COMMAND ----------

SELECT 
  
  PERCENTILE (VLPRECO,0,5)  AS PRECOMEDIANO,
    
    avg (vlpreco)  AS AVGPRECO,
    MAX (vlpreco)  AS MAXPRECO,
  avg (vlfrete)   AS AVGFRETE,
    MAX (vlfrete)   AS MAXFRETE,
    MIN (vlfrete)   AS MINFRETE
           
  FROM silver_olist.item_pedido

-- COMMAND ----------

-- ARREDONDAR - ROUND  - INT E CAST PROCURAR NO GOOGLE

SELECT 
  
  INT(PERCENTILE (VLPRECO,0,5))  AS PRECOMEDIANO,
    
    ROUND(avg (vlpreco),2)  AS AVGPRECO,
    INT(MAX (vlpreco))  AS MAXPRECO,
    INT(avg (vlfrete))   AS AVGFRETE,
    MAX (vlfrete)   AS MAXFRETE,
    MIN (vlfrete)   AS MINFRETE
           
  FROM silver_olist.item_pedido

-- COMMAND ----------

SELECT *
  
           
  FROM silver_olist.CLIENTE

-- COMMAND ----------

--CONTAR CLIENTES SAO PAULO

SELECT COUNT (*)
  
           
  FROM silver_olist.CLIENTE
  
  WHERE descuf = 'SP'

-- COMMAND ----------

--- APRENDER GROUPBY

SELECT descuf,

COUNT (*)
             
  FROM silver_olist.CLIENTE
  
  GROUP BY descuf

  
  

