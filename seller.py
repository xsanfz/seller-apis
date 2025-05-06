import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина с Ozon.

    Args:
        last_id: ID последнего товара (пустая строка для начала)
        client_id: ID клиента в Ozon
        seller_token: Токен продавца

    Returns:
        dict: Список товаров и данные пагинации

    Examples:
        >>> get_product_list("", "client123", "token456")
        {'items': [...], 'total': 100}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает все артикулы товаров магазина на Ozon.

    Args:
        client_id: Номер клиента Ozon
        seller_token: Токен продавца

    Returns:
        list: Список всех артикулов товаров

    Examples:
        >>> get_offer_ids("client123", "token456")
        ["12345", "67890"]

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в личном кабинете Ozon.

    Args:
        prices: Список цен в формате:
            [{
                "offer_id": "12345",
                "price": "5990",
                "currency_code": "RUB"
            }, ...]
        client_id: ID вашего аккаунта в Ozon
        seller_token: Ваш API-ключ Ozon

    Returns:
        dict: Ответ от Ozon

    Examples:
        >>> update_price(
        ...     [{"offer_id": "123", "price": "5990", "currency_code": "RUB"}],
        ...     "12345",
        ...     "abc123"
        ... )
        {"result": [...], "status": "OK"}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на складе в личном кабинете Ozon.

    Args:
        stocks: Список остатков в формате:
            [{
                "offer_id": "12345",
                "stock": 10
            }, ...]
        client_id: ID вашего магазина в Ozon
        seller_token: Ваш API-ключ от Ozon

    Returns:
        dict: Ответ от Ozon

    Examples:
        >>> update_stocks(
        ...     [{"offer_id": "12345", "stock": 5}],
        ...     "12345-AB",
        ...     "token123xyz"
        ... )
        {"result": [...], "status": "OK"}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает и обрабатывает файл с остатками с сайта timeworld.ru.

    Returns:
        list: Список товаров с остатками и ценами

    Examples:
        >>> download_stock()
        [{'Код': '123', 'Количество': '5', 'Цена': '5990 руб.'}, ...]

    Raises:
        requests.exceptions.HTTPError: Если не удалось скачать файл
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков для загрузки в Ozon.

    Args:
        watch_remnants: Данные по остаткам с сайта поставщика
        offer_ids: Артикулы товаров на Ozon

    Returns:
        list: Готовый список для обновления остатков

    Examples:
        >>> create_stocks([{"Код": "123", "Количество": "5"}], ["123"])
        [{"offer_id": "123", "stock": 5}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Подготавливает список цен товаров для загрузки в маркетплейс.

    Args:
        watch_remnants: Товары от поставщика
        offer_ids: Артикулы товаров в вашем магазине на маркетплейсе

    Returns:
        list: Список цен для загрузки

    Examples:
        >>> create_prices(
        ...     [{"Код": "12345", "Цена": "5'990.00 руб."}],
        ...     ["12345", "67890"]
        ... )
        [{
            "auto_action_enabled": "UNKNOWN",
            "currency_code": "RUB",
            "offer_id": "12345",
            "old_price": "0",
            "price": "5990"
        }]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в формат, пригодный для загрузки на маркетплейсы.

    Args:
        price: Строка с ценой в формате поставщика

    Returns:
        str: Цена без лишних символов

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("1 299.99 руб.")
        '1299'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разбивает список на части по n элементов.

    Args:
        lst: Исходный список
        n: Количество элементов в части

    Returns:
        list: Генератор с частями списка

    Examples:
        >>> list(divide([1,2,3,4], 2))
        [[1,2], [3,4]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загружает цены товаров в Ozon.

    Args:
        watch_remnants: Список товаров с ценами от поставщика
        client_id: Ваш ID клиента Ozon
        seller_token: API-ключ продавца

    Returns:
        list: Список всех загруженных цен

    Examples:
        >>> await upload_prices(
        ...     [{"Код": "12345", "Цена": "5'990.00 руб."}],
        ...     "12345-AB",
        ...     "token123xyz"
        ... )
        [{
            "offer_id": "12345",
            "price": "5990",
            "currency_code": "RUB"
        }]

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров в Ozon.

    Args:
        watch_remnants: Список товаров от поставщика
        client_id: ID вашего аккаунта Ozon
        seller_token: API-токен продавца

    Returns:
        tuple: Кортеж из двух элементов:
            - list: Товары с ненулевым остатком
            - list: Все обновленные товары

    Examples:
        >>> await upload_stocks(
        ...     [{"Код": "12345", "Количество": "5"}],
        ...     "CLIENT-123",
        ...     "SELLER-TOKEN-456"
        ... )
        (
            [{"offer_id": "12345", "stock": 5}],
            [{"offer_id": "12345", "stock": 5}]
        )

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для синхронизации данных с Ozon.

    Использует переменные окружения:
        - SELLER_TOKEN: API-токен продавца Ozon
        - CLIENT_ID: ID аккаунта в Ozon

    Examples:
        >>> main()
        [INFO] Данные успешно загружены:
        [INFO] - Обновлено 45 товаров
        [INFO] - Из них в наличии: 32

    Raises:
        requests.exceptions.ReadTimeout: При превышении времени ожидания
        requests.exceptions.ConnectionError: При проблемах с соединением
        Exception: При других неожиданных ошибках
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
