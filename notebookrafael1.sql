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

  
  


-- COMMAND ----------

--- APRENDER GROUPBY   DISTINTOS

SELECT descuf,

COUNT (DISTINCT idclienteunico)
             
  FROM silver_olist.CLIENTE
  
  GROUP BY descuf

-- COMMAND ----------


SELECT *

             
  FROM silver_olist.pedido

-- COMMAND ----------

-- lista pedidos dez 2017
SELECT *

      
  FROM silver_olist.pedido
  
  where year(dtpedido) = 2017 and
        month (dtpedido) = 12
  

-- COMMAND ----------

-- lista pedidos dez 2017 e entregues com atraso  (macete converter para Date pq pode dar problema de vir errado por causa da hora)
SELECT *

      
  FROM silver_olist.pedido
  
  where year(dtpedido) = 2017 and
        month (dtpedido) = 12 and
        descsituacao = 'delivered' and
        date(dtentregue) > date(dtestimativaentrega)

-- COMMAND ----------

SELECT *

      
  FROM silver_olist.pedido

-- COMMAND ----------

SELECT *

      
  FROM silver_olist.pagamento_pedido

-- COMMAND ----------

-- pedidos com mais de 2 parcelas

SELECT *

      
  FROM silver_olist.pagamento_pedido
  
  where nrpacelas >= 2

-- COMMAND ----------

-- pedidos com mais de 2 parcelas  ou mais menores q 20 reais

SELECT *
      
  FROM silver_olist.pagamento_pedido
  
  where nrpacelas >= 2  and
  
  vlpagamento / nrpacelas < 20
  
  
  

-- COMMAND ----------

-- pedidos com mais de 2 parcelas  ou mais menores q 20 reais

SELECT *,

 vlpagamento / nrpacelas AS VALORPARCELA
      
  FROM silver_olist.pagamento_pedido
  
  where nrpacelas >= 2  and
  
  vlpagamento / nrpacelas < 20
  

-- COMMAND ----------

-- pedidos com mais de 2 parcelas  ou mais menores q 20 reais  - COM ROUND

SELECT *,

 ROUND(vlpagamento / nrpacelas,2) AS VALORPARCELA
      
  FROM silver_olist.pagamento_pedido
  
  where nrpacelas >= 2  and
  
  vlpagamento / nrpacelas < 20

-- COMMAND ----------

-- SELECIONE OS ITENS DE PEDIDOS (FRETE) E DEFINA EM GRUPOS EM UMA NOVA COLUNA


SELECT *

      
  FROM silver_olist.ITEM_PEDIDO
  




-- COMMAND ----------

-- SELECIONE OS ITENS DE PEDIDOS (FRETE) E DEFINA EM GRUPOS EM UMA NOVA COLUNA -- PRIMEIRA COISA CALCULAR VALOR TOTAL QUE É PREÇO MAIS FRETE


SELECT *,

  VLPRECO + VLFRETE AS VLTOTAL
      
  FROM silver_olist.ITEM_PEDIDO
  
  
  

-- COMMAND ----------

-- SELECIONE OS ITENS DE PEDIDOS (FRETE) E DEFINA EM GRUPOS EM UMA NOVA COLUNA -- AGORA MAIS 1 COLUNA DE % FRETE


SELECT *,

  VLPRECO + VLFRETE AS VLTOTAL,
  VLFRETE / (VLPRECO + VLFRETE) AS PERCFRETE
      
  FROM silver_olist.ITEM_PEDIDO
  
  

-- COMMAND ----------

-- SELECIONE OS ITENS DE PEDIDOS (FRETE) E DEFINA EM GRUPOS EM UMA NOVA COLUNA -- AGORA USAR UM CASE WHEN EM CIMA DO PERCFRETE


SELECT *,

--   vlpreco + vlfrete AS vltotal,
--   vlfrete / (vlpreco + vlfrete) AS percfrete,
  
  CASE WHEN  
  
      VLFRETE / (VLPRECO + VLFRETE) <= 0.1 THEN '10%'
      VLFRETE / (VLPRECO + VLFRETE) <= 0.25 THEN '10% A 25%'
      VLFRETE / (VLPRECO + VLFRETE) <= 0.5 THEN '50%'
      
      ELSE '+50%'
      
      END AS DESFRETEPACOTE
      
  FROM silver_olist.ITEM_PEDIDO
  

-- COMMAND ----------

---  order by


SELECT *

      
  FROM silver_olist.cliente
  


-- COMMAND ----------

---  order by pela quantidade de clientes


SELECT 
 
 descuf,
 count (distinct idclienteunico)  as qtdclienteestado
      
  FROM silver_olist.cliente
  
  group by descuf
  order by descuf
  
  

-- COMMAND ----------

---  order by pela quantidade de clientes  (crescente)


SELECT 
 
 descuf,
 count (distinct idclienteunico)  as qtdclienteestado
      
  FROM silver_olist.cliente
  
  group by descuf
  order by qtdclienteestado

-- COMMAND ----------

---  order by pela quantidade de clientes  (decrescente)


SELECT 
 
 descuf,
 count (distinct idclienteunico)  as qtdclienteestado
      
  FROM silver_olist.cliente
  
  group by descuf
  order by qtdclienteestado DESC

-- COMMAND ----------

---  order by pela quantidade de clientes  (decrescente)  APARECENDO APENAS A PRIMEIRA LINHA depois que tiver ordenado


SELECT 
 
 descuf,
 count (distinct idclienteunico)  as qtdclienteestado
      
  FROM silver_olist.cliente
  
  group by descuf
  order by qtdclienteestado DESC
  
  LIMIT 1

-- COMMAND ----------

--- case when PERCENTUAIS

SELECT *, 
 vlPreco + vlFrete AS vlTotal, 
 vlFrete / (vlPreco + vlFrete) AS pctFrete,
 
 CASE WHEN vlFrete / (vlPreco + vlFrete) <= 0.1 THEN '10%' 
      WHEN vlFrete / (vlPreco + vlFrete) <= 0.25 THEN '10% A 25%'
      WHEN vlFrete / (vlPreco + vlFrete) <= 0.5 THEN '25% A 50%' 
      ELSE '+50%' 
      
      END AS descFretePct 
      
      FROM silver_olist.item_pedido

-- COMMAND ----------

--- HAVING  DE CONTAGEM DE VENDEDOR

SELECT *
 
     
  FROM silver_olist.VENDEDOR
  
 
 



-- COMMAND ----------

--- HAVING  DE CONTAGEM DE VENDEDOR AGRUPANDO POR ESTADO

SELECT
 descuf,
 count(idvendedor)
     
  FROM silver_olist.VENDEDOR
  
  group by descuf
  

-- COMMAND ----------

--- HAVING  DE CONTAGEM DE VENDEDOR AGRUPANDO POR ESTADO -  O HAVING É UM FILTRO DEPOIS DA AGREGACAO

SELECT
 descuf,
 count(idvendedor)
     
  FROM silver_olist.VENDEDOR
  
  group by descuf
  
  HAVING  count(idvendedor)  >= 100

-- COMMAND ----------

--- HAVING  DE CONTAGEM DE VENDEDOR AGRUPANDO POR ESTADO -  O HAVING É UM FILTRO DEPOIS DA AGREGACAO COM NOME DA COLUNA, COM MAIS DE 100 VENDEDORES

SELECT
 descuf,
 count(idvendedor) AS QTDVENDEDOR
     
  FROM silver_olist.VENDEDOR
  
  group by descuf
  
  HAVING  count(idvendedor)  >= 100
  
  ORDER BY QTDVENDEDOR DESC

-- COMMAND ----------

--- HAVING  DE CONTAGEM DE VENDEDOR AGRUPANDO POR ESTADO -  O HAVING É UM FILTRO DEPOIS DA AGREGACAO COM NOME DA COLUNA COM MAIS DE 100 VENDEDORES NA REGIÃO SUDESTE   --  ( O WHERE SEMPRE VEM LOGO APOS O FROM)

--PARA COLOCOCAR MAIS DE 1 ORDER BY COLOCA A VIRGULA,    PARA ORDEM CRESCENT usa ASC

SELECT
 descuf,
 count(idvendedor) AS QTDVENDEDOR
     
  FROM silver_olist.VENDEDOR
  
  WHERE descuf IN ('SP','RJ','MG','ES')
  
  group by descuf
  
  HAVING  count(idvendedor)  >= 100
  
  ORDER BY QTDVENDEDOR DESC

-- COMMAND ----------

--- JOIN

SELECT T1.*,
       T2.descuf
       
FROM silver_olist.pedido  AS T1

LEFT JOIN silver_olist.cliente  AS T2

ON T1.idcliente = T2.idcliente







-- COMMAND ----------


