import { WebsocketClient, RestClientV5 } from 'bybit-api';
import WebSocket from 'ws';
import * as dotenv from 'dotenv';

dotenv.config();

// ==========================================
// НАСТРОЙКИ СТРАТЕГИИ
// ==========================================
const SYMBOL_BYBIT = 'BTCUSDT';
const SYMBOL_BINANCE = 'btcusdt'; // Binance требует нижний регистр для WS
const CONSOLIDATION_WINDOW_MS = 15 * 60 * 1000; // Окно анализа (15 минут)

// Пороги для триггера
const MAX_PRICE_SPREAD_PCT = 1.5; // Максимальная ширина боковика (в %)
const OI_GROWTH_THRESHOLD_PCT = 2.0; // Минимальный рост Открытого Интереса (в %)
const CVD_DIVERGENCE_THRESHOLD = -50.0; // Насколько сильно дельта должна упасть (в базовой валюте, например BTC)

// Данные авторизации Bybit
const BYBIT_API_KEY = process.env.BYBIT_API_KEY || 'YOUR_API_KEY';
const BYBIT_API_SECRET = process.env.BYBIT_API_SECRET || 'YOUR_SECRET';

// ==========================================
// ХРАНИЛИЩЕ ДАННЫХ В ПАМЯТИ (Храним за последние 15 минут)
// ==========================================
interface TickData {
    timestamp: number;
    price: number;
    binanceCvd: number;
    bybitCvd: number;
    bybitOI: number;
}

let tickHistory: TickData[] = [];

// Текущие кумулятивные значения (счетчики)
let currentBinanceCvd = 0;
let currentBybitCvd = 0;
let currentBybitOI = 0;
let lastKnownPrice = 0;
let isPositionOpen = false;

// Инициализация REST-клиента Bybit для выставления ордеров
const bybitRestClient = new RestClientV5({
    key: BYBIT_API_KEY,
    secret: BYBIT_API_SECRET,
    testnet: false // Смените на true, если тестируете на демо-счете
});

// ==========================================
// 1. СБОР ДАННЫХ: ВЕБСОКЕТ BINANCE (Агрегированные сделки)
// ==========================================
function connectBinanceWS() {
    const binanceWsUrl = `wss://://binance.com{SYMBOL_BINANCE}@aggTrade`;
    const ws = new WebSocket(binanceWsUrl);

    ws.on('message', (data: string) => {
        const msg = JSON.parse(data);
        const volume = parseFloat(msg.q); // Количество монет в сделке
        const isBuyerMarket = !msg.m;      // msg.m = true означает, что это был Maker-продавца (то есть удар по рынку от продавца)

        if (isBuyerMarket) {
            currentBinanceCvd += volume;
        } else {
            currentBinanceCvd -= volume;
        }
    });

    ws.on('close', () => setTimeout(connectBinanceWS, 5000)); // Автореконнект
}

// ==========================================
// 2. СБОР ДАННЫХ: ВЕБСОКЕТ BYBIT (Сделки и Открытый Интерес)
// ==========================================
function connectBybitWS() {
    const bybitWs = new WebsocketClient({ market: 'v5' });

    // Подписываемся на сделки (Trade) и тикер (содержит Open Interest)
    bybitWs.subscribeV5([`publicTrade.${SYMBOL_BYBIT}`, `tickers.${SYMBOL_BYBIT}`], 'linear');

    bybitWs.on('update', (response) => {
        const topic = response.topic;

        // Обработка рыночных сделок для Bybit CVD
        if (topic.startsWith('publicTrade')) {
            for (const trade of response.data) {
                const volume = parseFloat(trade.v);
                const side = trade.S; // 'Buy' или 'Sell'
                lastKnownPrice = parseFloat(trade.p);

                if (side === 'Buy') {
                    currentBybitCvd += volume;
                } else if (side === 'Sell') {
                    currentBybitCvd -= volume;
                }
            }
        }

        // Обработка обновлений Открытого Интереса
        if (topic.startsWith('tickers')) {
            if (response.data.openInterest) {
                currentBybitOI = parseFloat(response.data.openInterest);
            }
        }
    });

    bybitWs.on('close', () => setTimeout(connectBybitWS, 5000));
}

// ==========================================
// 3. АНАЛИТИЧЕСКИЙ СЕРДЦЕВИННЫЙ ЦИКЛ (Каждую секунду)
// ==========================================
function startAnalysisLoop() {
    setInterval(async () => {
        if (lastKnownPrice === 0 || currentBybitOI === 0) return; // Ждем первичных данных

        const now = Date.now();

        // 1. Добавляем текущий снимок рынка в историю
        tickHistory.push({
            timestamp: now,
            price: lastKnownPrice,
            binanceCvd: currentBinanceCvd,
            bybitCvd: currentBybitCvd,
            bybitOI: currentBybitOI
        });

        // 2. Очищаем историю от старых данных (старше 15 минут)
        const cutoffTime = now - CONSOLIDATION_WINDOW_MS;
        tickHistory = tickHistory.filter(tick => tick.timestamp >= cutoffTime);

        if (tickHistory.length < 60) return; // Нам нужно накопить хотя бы 1 минуту данных

        // 3. Расчет метрик за исторический период
        const initialTick = tickHistory[0];
        const prices = tickHistory.map(t => t.price);
        
        const maxPrice = Math.max(...prices);
        const minPrice = Math.min(...prices);
        
        // Ширина боковика в процентах
        const priceSpreadPct = ((maxPrice - minPrice) / minPrice) * 100;

        // Изменение метрик с начала окна консолидации
        const binanceCvdChange = currentBinanceCvd - initialTick.binanceCvd;
        const bybitCvdChange = currentBybitCvd - initialTick.bybitCvd;
        const oiChangePct = ((currentBybitOI - initialTick.bybitOI) / initialTick.bybitOI) * 100;

        console.log(`[Мониторинг] Боковик: ${priceSpreadPct.toFixed(2)}% | Изм. OI: ${oiChangePct.toFixed(2)}% | CVD Bin: ${binanceCvdChange.toFixed(1)} | CVD Byb: ${bybitCvdChange.toFixed(1)}`);

        // 4. ПРОВЕРКА УСЛОВИЙ СТРАТЕГИИ
        if (!isPositionOpen) {
            const isConsolidation = priceSpreadPct <= MAX_PRICE_SPREAD_PCT;
            const isOiGrowing = oiChangePct >= OI_GROWTH_THRESHOLD_PCT;
            
            // Проверяем, что на обеих биржах идет сильный шортовый напор по рынку
            const isHeavySelling = binanceCvdChange < CVD_DIVERGENCE_THRESHOLD && bybitCvdChange < CVD_DIVERGENCE_THRESHOLD;
            
            // Цена при этом зажата и находится ближе к нижней границе боковика (лимитный покупатель держит удар)
            const isPriceHolding = lastKnownPrice <= (minPrice + (maxPrice - minPrice) * 0.3);

            if (isConsolidation && isOiGrowing && isHeavySelling && isPriceHolding) {
                console.log('🚀 ОБНАРУЖЕН ЛИМИТНЫЙ ПОКУПАТЕЛЬ! Входим в LONG...');
                await executeLongOrder(lastKnownPrice, minPrice);
            }
        }
    }, 1000); // Проверка каждую секунду
}

// ==========================================
// 4. ИСПОЛНЕНИЕ ОРДЕРОВ НА BYBIT (Защита 1к3)
// ==========================================
async function executeLongOrder(entryPrice: number, lowPriceOfConsolidation: number) {
    try {
        isPositionOpen = true;

        // Считаем размер Стоп-Лосса (под нижнюю границу боковика)
        const stopLossPrice = Math.round((lowPriceOfConsolidation * 0.998) * 10) / 10; // -0.2% запаса под лоу боковика
        const stopLossDistance = entryPrice - stopLossPrice;

        if (stopLossDistance <= 0) {
            isPositionOpen = false;
            return;
        }

        // Вычисляем Тейк-Профит ровно 1 к 3
        const takeProfitPrice = Math.round((entryPrice + (stopLossDistance * 3)) * 10) / 10;

        console.log(`[Ордер] Вход: ${entryPrice} | Стоп: ${stopLossPrice} | Тейк (1к3): ${takeProfitPrice}`);

        // Отправляем ордер на Bybit (Рыночный или лимитный с привязанными SL/TP)
        const response = await bybitRestClient.submitOrder({
            category: 'linear',
            symbol: SYMBOL_BYBIT,
            side: 'Buy',
            orderType: 'Market', // Используем Market для мгновенного входа по сигналу
            qty: '0.01',       // Размер позиции (Укажите ваш в зависимости от риск-менеджмента)
            timeInForce: 'GTC',
            takeProfit: takeProfitPrice.toString(),
            stopLoss: stopLossPrice.toString(),
            tpTriggerBy: 'LastPrice',
            slTriggerBy: 'LastPrice'
        });

        if (response.retCode === 0) {
            console.log('✅ Позиция успешно открыта на бирже со связанными SL/TP 1к3.');
            
            // Для теста сбрасываем флаг через 20 минут, в реале нужно слушать приватный вебсокет обновлений позиций
            setTimeout(() => { isPositionOpen = false; }, 20 * 60 * 1000);
        } else {
            console.error('❌ Ошибка биржи при выставлении ордера:', response.retMsg);
            isPositionOpen = false;
        }

    } catch (error) {
        console.error('❌ Критическая ошибка модуля исполнения:', error);
        isPositionOpen = false;
    }
}

// ==========================================
// ТОЧКА ВХОДА В ПРИЛОЖЕНИЕ
// ==========================================
function main() {
    console.log('Запуск мультивалютного волюметрического робота...');
    connectBinanceWS();
    connectBybitWS();
    startAnalysisLoop();
}

main();
