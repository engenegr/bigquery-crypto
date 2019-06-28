#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
import os
import pandas
import hashlib

from datetime import datetime

def get_bitcoin_template(query_type: str, limit: int, *args, **kwargs)-> str:
    address = '0x0'
    if 'address' in kwargs:
        try:
            address = str(kwargs['address'])
        except:
            pass
    start_block = 0
    if 'start_block' in kwargs:
        try:
            start_block = int(kwargs['start_block'])
        except:
            pass
    if query_type == 'transactions':
        transactions = """
            SELECT * 
            FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions
            WHERE transactions.block_number >= {0:.0f}
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return transactions
    if query_type == 'transactions-daily':
        transactions = """WITH time AS
            (
                SELECT DATE(transactions.block_timestamp) AS trans_time
                , transactions.hash as transaction_id
                FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions
                WHERE transactions.block_number >= {0:.0f}
            )
            SELECT COUNT(transaction_id) AS unique_transid_num, 
                    EXTRACT(DAY FROM trans_time) AS DAY
            FROM time
            GROUP BY DAY
            ORDER BY unique_transid_num DESC
            LIMIT {1:.0f}       
            """.format(start_block, limit)
        return transactions
    if query_type == 'transactions-monthly':
        transactions = """WITH time as
            (
                    SELECT DATE(transactions.block_timestamp) AS trans_time
                    , transactions.hash as transaction_id
                    FROM `bigquery-public-data.crypto_bitcoin.transactions` AS transactions
                    WHERE transactions.block_number >= {1:.0f}
            )
            SELECT COUNT(transaction_id) AS unique_transid_num, 
                    EXTRACT(MONTH FROM trans_time) AS MONTH
            FROM time
            GROUP BY MONTH
            ORDER BY unique_transid_num DESC
            LIMIT {1:.0f}          
            """.format(start_block, limit)
        return transactions

    if query_type == 'addresses_count':
        query = """
        SELECT COUNT(*) AS Count,
          timestamp AS Ts
        FROM
        (
            SELECT DISTINCT
              ARRAY_TO_STRING(outputs.addresses, ',') AS address
            , outputs.block_timestamp AS timestamp
            FROM `bigquery-public-data.crypto_bitcoin.outputs` AS outputs
            WHERE outputs.block_number >= {0:.0f}
        )  
        GROUP BY Ts        
        """.format(start_block, limit)
        return query


def get_ethereum_template(query_type: str, limit: int, *args, **kwargs)-> str:
    address = '0x0'
    if 'address' in kwargs:
        try:
            address = str(kwargs['address'])
        except:
            pass
    start_block = 0
    if 'start_block' in kwargs:
        try:
            start_block = int(kwargs['start_block'])
        except:
            pass
    if query_type == 'transactions':
        transactions = """
            SELECT * 
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE transactions.block_number >= {0:.0f}
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return transactions
    if query_type == 'transactions-to':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE TRUE
                AND transactions.to_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'transactions-from':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.transactions` AS transactions
            WHERE TRUE
                AND transactions.from_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'token-transfers':
        transfers = """
            SELECT 
                SUM(CAST(value AS NUMERIC)/POWER(10,18)) AS daily_weight,
                DATE(timestamp) AS tx_date
            FROM 
                `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers, 
                `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
            WHERE token_transfers.block_number >= {0:.0f}
            AND token_transfers.block_number = blocks.number
            AND token_transfers.token_address = "{1:}"
            GROUP BY tx_date
            ORDER BY tx_date
        """.format(start_block, address)
        return transfers
    if query_type == 'top-erc20':
        top = """
            SELECT contracts.address, COUNT(1) AS tx_count
            FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
            JOIN `bigquery-public-data.ethereum_blockchain.transactions` AS transactions ON ((transactions.block_number >= {0:.0f}) AND (transactions.to_address = contracts.address))
            WHERE contracts.is_erc20 = TRUE
            GROUP BY contracts.address
            ORDER BY tx_count DESC
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return top
    if query_type == 'top-erc20-transfers':
        top = """
            SELECT contracts.address, COUNT(1) AS tx_count
            FROM `bigquery-public-data.ethereum_blockchain.contracts` AS contracts
            JOIN `bigquery-public-data.ethereum_blockchain.token_transfers` AS transactions ON ((transactions.block_number >= {0:.0f}) AND (transactions.token_address = contracts.address))
            WHERE contracts.is_erc20 = TRUE
            GROUP BY contracts.address
            ORDER BY tx_count DESC
            LIMIT {1:.0f}
        """.format(start_block, limit)
        return top
    if query_type == 'daily-erc20-transfers':
        top = """
            SELECT 
            COUNT(1) AS tx_count,
            DATE(block_timestamp) AS tx_date
            FROM 
            `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers
            WHERE TRUE
                AND token_transfers.token_address = "{0:}"
                AND token_transfers.block_number >= {1:.0f}
            GROUP BY tx_date
            ORDER BY tx_date
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return top
    if query_type == 'daily-erc20-transfers-volume':
        top = """
            SELECT 
            SUM(CAST(value AS NUMERIC)) AS volume,
            DATE(block_timestamp) AS tx_date
            FROM 
            `bigquery-public-data.ethereum_blockchain.token_transfers` AS token_transfers
            WHERE TRUE
                AND token_transfers.token_address = "{0:}"
                AND token_transfers.block_number >= {1:.0f}
            GROUP BY tx_date
            ORDER BY tx_date
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return top
    if query_type == 'transactions-transfer':
        transactions = """
            SELECT *
            FROM `bigquery-public-data.ethereum_blockchain.token_transfers` AS transactions
            WHERE TRUE
                AND transactions.token_address = "{0:}"
                AND transactions.block_number >= {1:.0f}
            LIMIT {2:.0f}
        """.format(address, start_block, limit)
        return transactions
    if query_type == 'gas-cost':
        gas_cost = """
            SELECT 
              SUM(value/POWER(10,18)) AS sum_tx_ether,
              AVG(gas_price*(receipt_gas_used/POWER(10,18))) AS avg_tx_gas_cost,
              DATE(timestamp) AS tx_date
            FROM
              `bigquery-public-data.ethereum_blockchain.transactions` AS transactions,
              `bigquery-public-data.ethereum_blockchain.blocks` AS blocks
            WHERE TRUE
              AND transactions.block_number = blocks.number
              AND receipt_status = 1
              AND value > 0
            GROUP BY tx_date
            HAVING tx_date >= '2018-01-01' AND tx_date <= '2018-12-31'
            ORDER BY tx_date
        """
        return gas_cost