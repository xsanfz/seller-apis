import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает одну страницу товаров магазина с Яндекс.Маркета.

    Args:
        page: Токен для пагинации (пустая строка для первой страницы)
        campaign_id: Номер магазина в Маркете
        access_token: Ключ доступа к API

    Returns:
        dict: Словарь с товарами и данными пагинации

    Examples:
        >>> get_product_list("", "123", "abc123")
        {'offerMappingEntries': [...], 'paging': {...}}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет количество товаров на складе Маркета.

    Args:
        stocks: Список товаров с новыми остатками
        campaign_id: Номер магазина
        access_token: Ключ API

    Returns:
        dict: Ответ от Маркета об успешном обновлении

    Examples:
        >>> update_stocks([{"sku": "123", "count": 10}], "123", "abc123")
        {'status': 'OK'}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Меняет цены товаров в Маркете.

    Args:
        prices: Список товаров с новыми ценами
        campaign_id: Номер магазина
        access_token: Ключ API

    Returns:
        dict: Ответ об обновлении цен

    Examples:
        >>> update_price([{"id": "123", "price": 5990}], "123", "abc123")
        {'status': 'OK'}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает все артикулы товаров из Яндекс.Маркета.

    Собирает полный список артикулов товаров магазина, обходя все страницы.

    Args:
        campaign_id: Номер вашей кампании в Маркете
        market_token: Токен доступа к API

    Returns:
        list: Список всех артикулов товаров

    Examples:
        >>> get_offer_ids("12345", "AbCdEf123")
        ["1001", "1002", "1003"]

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует список остатков товаров для загрузки в Яндекс.Маркет.

    На основе данных поставщика создает список товаров с обновленными остатками.
    Для товаров, которых нет в данных поставщика, устанавливает остаток 0.

    Args:
        watch_remnants: Список товаров от поставщика
        offer_ids: Список артикулов товаров в Маркете
        warehouse_id: ID склада в Маркете

    Returns:
        list: Готовый список для обновления остатков

    Examples:
        >>> create_stocks(
        ...     [{"Код": "123", "Количество": "5"}],
        ...     ["123", "456"],
        ...     "WH-123"
        ... )
        [
            {"sku": "123", "warehouseId": "WH-123", "items": [{"count": 5}]},
            {"sku": "456", "warehouseId": "WH-123", "items": [{"count": 0}]}
        ]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен товаров для загрузки в Яндекс.Маркет.

    На основе данных поставщика создает список товаров с обновленными ценами.
    Включает только товары, которые есть как у поставщика, так и в Маркете.

    Args:
        watch_remnants: Список товаров от поставщика
        offer_ids: Список артикулов товаров в Маркете

    Returns:
        list: Список цен для обновления

    Examples:
        >>> create_prices(
        ...     [{"Код": "123", "Цена": "5'990.00 руб."}],
        ...     ["123", "456"]
        ... )
        [{
            "id": "123",
            "price": {"value": 5990, "currencyId": "RUR"}
        }]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены товаров в Яндекс.Маркете.

    Получает актуальные цены из данных поставщика и загружает их в указанную кампанию.
    Автоматически разбивает список товаров на пакеты по 500 позиций.

    Args:
        watch_remnants: Список товаров от поставщика
        campaign_id: ID кампании в Маркете
        market_token: API-токен для доступа к Яндекс.Маркету

    Returns:
        list: Список всех успешно загруженных цен

    Examples:
        >>> await upload_prices(
        ...     [{"Код": "12345", "Цена": "5'990.00 руб."}],
        ...     "123456",
        ...     "market_token_abc123"
        ... )
        [{
            "id": "12345",
            "price": {
                "value": 5990,
                "currencyId": "RUR"
            }
        }]

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет остатки товаров для указанного склада в Яндекс.Маркете.

    Получает текущие остатки товаров из данных поставщика и синхронизирует их с указанным складом.
    Автоматически разбивает большой список на части по 2000 товаров.

    Args:
        watch_remnants: Список товаров от поставщика
        campaign_id: ID кампании в Яндекс.Маркете
        market_token: API-токен доступа
        warehouse_id: ID склада в Маркете

    Returns:
        tuple: Кортеж из двух элементов:
            - list: Товары с ненулевым остатком
            - list: Все обновленные товары

    Examples:
        >>> await upload_stocks(
        ...     [{"Код": "12345", "Количество": "5"}],
        ...     "CAMPAIGN123",
        ...     "TOKEN456",
        ...     "WAREHOUSE789"
        ... )
        (
            [{"sku": "12345", "warehouseId": "WAREHOUSE789", "items": [{"count": 5}]}],
            [{"sku": "12345", "warehouseId": "WAREHOUSE789", "items": [{"count": 5}]}]
        )

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился ошибкой
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для управления процессом синхронизации данных с Яндекс.Маркета.

    Выполняет полный цикл обновления данных:
    1. Загружает актуальные остатки с сайта поставщика
    2. Синхронизирует данные с Яндекс.Маркет (FBS и DBS)
    3. Обрабатывает возможные ошибки соединения

    Использует переменные окружения:
        - MARKET_TOKEN: Токен API Яндекс.Маркета
        - FBS_ID: ID FBS-кампании
        - DBS_ID: ID DBS-кампании
        - WAREHOUSE_FBS_ID: ID FBS-склада
        - WAREHOUSE_DBS_ID: ID DBS-склада

    Examples:
        >>> main()
        [INFO] Начата синхронизация с Яндекс.Маркет...
        [INFO] FBS: обновлено 15 товаров
        [INFO] DBS: обновлено 12 товаров

    Raises:
        requests.exceptions.ReadTimeout: При превышении времени ожидания
        requests.exceptions.ConnectionError: При проблемах с соединением
        Exception: При других неожиданных ошибках
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
