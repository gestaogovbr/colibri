/*
Resumo anual de compras públicas, para conferência com os números públicos do
painel "PNCP em Números" (https://www.gov.br/pncp/pt-br/painel-pncp).
*/

{{ config(
    tags=['marts', 'pncp', 'comprasgov']
) }}

SELECT
    ano_compra,
    COUNT(DISTINCT cod_compra) AS qtd_compras,
    SUM(valor_total_estimado)  AS valor_total_estimado,
    SUM(valor_total_homologado) AS valor_total_homologado
FROM {{ ref('int_pncp_comprasgov__compras') }}
WHERE is_current
GROUP BY ano_compra
ORDER BY ano_compra
