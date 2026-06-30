# Robozinho_de_Investimento
Machine learning de investimentos voltada para Bancos

from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime, timedelta
from binance import Client
import yfinance as yf
import time

# ==========================================
# 1. CLASSE ABSTRATA MÃE (USANDO GERADORES)
# ==========================================
class ExtratorStreamingBase(ABC):
    
    def __init__(self, ticker: str, intervalo: str):
        self.ticker = ticker
        self.intervalo = intervalo

    @abstractmethod
    def streaming_dados(self, inicio: datetime, fim: datetime):
        """
        Método abstrato que deve usar 'yield' para entregar os dados
        em partes (linha por linha ou bloco por bloco) sem acumular na memória.
        """
        pass

    def _padronizar_linha(self, colunas) -> dict:
        """Garante que cada linha emitida pelo feed tenha as mesmas chaves."""
        # Implementado nas classes filhas para mapear os dados brutos
        pass

class ExtratorStreamingBinance(ExtratorStreamingBase):
    
    def streaming_dados(self, inicio: datetime, fim: datetime):
        client = Client()
        ini_str = inicio.strftime('%d %b, %Y %H:%M:%S')
        fim_str = fim.strftime('%d %b, %Y %H:%M:%S')
        
        # O generator nativo da Binance puxa os dados em lotes da API de forma eficiente
        gerador_binance = client.get_historical_klines_generator(
            self.ticker, self.intervalo, ini_str, fim_str
        )
        
        for kline in gerador_binance:
            # Transforma a linha bruta em um dicionário padronizado
            linha_padrao = {
                "Timestamp": pd.to_datetime(kline[0], unit='ms'),
                "Ativo": self.ticker,
                "Abertura": float(kline[1]),
                "Maxima": float(kline[2]),
                "Minima": float(kline[3]),
                "Fechamento": float(kline[4]),
                "Volume": float(kline[5])
            }
            # O yield envia uma linha por vez para quem chamou a função
            yield linha_padrao

class ExtratorStreamingYahoo(ExtratorStreamingBase):
    
    def streaming_dados(self, inicio: datetime, fim: datetime):
        data_atual = inicio
        # Se for dados de minutos, processa dia após dia para economizar memória
        passo_tempo = timedelta(days=1) if "m" in self.intervalo else timedelta(days=30)
        
        while data_atual < fim:
            proxima_data = min(data_atual + passo_tempo, fim)
            
            ini_str = data_atual.strftime('%Y-%m-%d')
            fim_str = proxima_data.strftime('%Y-%m-%d')
            
            # Baixa apenas o bloco (janela) atual
            df_bloco = yf.download(self.ticker, start=ini_str, end=fim_str, interval=self.intervalo, progress=False)
            
            if not df_bloco.empty:
                df_bloco = df_bloco.reset_index()
                if isinstance(df_bloco.columns, pd.MultiIndex):
                    df_bloco.columns = df_bloco.columns.get_level_values(0)
                
                # Itera sobre as linhas do bloco atual e faz o yield de cada uma
                for _, row in df_bloco.iterrows():
                    linha_padrao = {
                        "Timestamp": row[df_bloco.columns[0]], # Primeira coluna costuma ser a Data/Datetime
                        "Ativo": self.ticker,
                        "Abertura": float(row['Open']),
                        "Maxima": float(row['High']),
                        "Minima": float(row['Low']),
                        "Fechamento": float(row['Close']),
                        "Volume": float(row['Volume'])
                    }
                    yield linha_padrao
            
            # Avança a janela de tempo
            data_atual = proxima_data

if __name__ == "__main__":
    # Período de teste curto para demonstração
    inicio = datetime(2026, 6, 1)
    fim = datetime(2026, 6, 5)

    # Instanciando nossos feeds abstratos
    feeds = [
        ExtratorStreamingBinance(ticker="BTCUSDT", intervalo=Client.KLINE_INTERVAL_1MINUTE),
        ExtratorStreamingYahoo(ticker="AAPL", intervalo="1m")
    ]

    for feed in feeds:
        print(f"\n--- Conectando ao Feed de: {feed.ticker} ---")
        
        # O método 'streaming_dados' retorna um gerador, ele NÃO roda o código todo ainda
        fluxo_tempo_real = feed.streaming_dados(inicio, fim)
        
        # Consumindo o feed linha por linha (Simulação de tempo real)
        contador = 0
        for linha in fluxo_tempo_real:
            # Aqui você faria o cálculo do seu indicador, backend, ou bot
            print(f"📡 [FEED] {linha['Timestamp']} | {linha['Ativo']} -> Close: ${linha['Fechamento']:.2f} | Vol: {linha['Volume']}")
            
            # Opcional: Adiciona um pequeno delay para simular o tempo real visualmente
            time.sleep(0.1) 
            
            contador += 1
            if contador >= 5: # Limitador no exemplo para não estender o log infinitamente
                print("... fluxo continua em background sem encher a RAM ...")
                break

import time
from collections import deque
from typing import NamedTuple, Union
from datetime import datetime

# ==============================================================================
# 1. DEFINIÇÃO DOS EVENTOS (Imutáveis e de Baixa Latência usando NamedTuples)
# ==============================================================================

class TickEvent(NamedTuple):
    timestamp: datetime
    symbol: str
    bid: float
    ask: float

class SignalEvent(NamedTuple):
    symbol: str
    direction: str  # 'BUY' ou 'SELL'
    strength: float # Métrica para tamanho de posição

class OrderEvent(NamedTuple):
    symbol: str
    order_type: str # 'MARKET' ou 'LIMIT'
    quantity: int
    direction: str  # 'BUY' ou 'SELL'

class FillEvent(NamedTuple):
    timestamp: datetime
    symbol: str
    quantity: int
    direction: str
    fill_cost: float
    commission: float

# Tipo unificado para type hinting da fila
Event = Union[TickEvent, SignalEvent, OrderEvent, FillEvent]

# ==============================================================================
# 2. COMPONENTES DO ECOSSISTEMA
# ==============================================================================

class Strategy:
    """Consome TICKs e gera SIGNALs com base em lógica matemática genérica."""
    def __init__(self, events_queue: deque):
        self.events_queue = events_queue

    def calculate_signals(self, tick: TickEvent):
        # Exemplo de lógica genérica: Se o preço spread (Ask/Bid) for válido, gera sinal
        # Substitua por sua lógica de Média Móvel, RSI, etc.
        if tick.ask > 0:
            signal = SignalEvent(symbol=tick.symbol, direction="BUY", strength=1.0)
            self.events_queue.append(signal)


class Portfolio:
    """Consome SIGNALs e TICKs para gerenciar risco e gerar ORDERs."""
    def __init__(self, events_queue: deque):
        self.events_queue = events_queue
        self.positions = {} # {SYMBOL: QUANTITY}

    def update_signal(self, signal: SignalEvent):
        # Lógica simples de Money Management (Ex: Boiada fixa de 100 unidades)
        order = OrderEvent(
            symbol=signal.symbol, 
            order_type="MARKET", 
            quantity=100, 
            direction=signal.direction
        )
        self.events_queue.append(order)


class ExecutionHandler:
    """Consome ORDERs, simula a execução no mercado e gera FILLs."""
    def __init__(self, events_queue: deque):
        self.events_queue = events_queue

    def execute_order(self, order: OrderEvent):
        # Simula uma execução instantânea a mercado (Market Order)
        # Em produção, conectar aqui o Websocket/Fix da Corretora ou Binance
        custo_simulado = 150.00  # Em um backtest, você usaria o último preço do Tick
        comissao = custo_simulado * 0.0003 # 0.03% taxa
        
        fill = FillEvent(
            timestamp=datetime.now(),
            symbol=order.symbol,
            quantity=order.quantity,
            direction=order.direction,
            fill_cost=custo_simulado,
            commission=comissao
        )
        self.events_queue.append(fill)

# ==============================================================================
# 3. ENGINE CENTRAL (EVENT-DRIVEN LOOP)
# ==============================================================================

class TradingEngine:
    def __init__(self):
        self.events_queue = deque()
        self.strategy = Strategy(self.events_queue)
        self.portfolio = Portfolio(self.events_queue)
        self.execution = ExecutionHandler(self.events_queue)

    def run(self, feed_simulado: list[TickEvent]):
        print("🚀 Iniciando Motor Orientado a Eventos...")
        
        # Otimização: Trazer os métodos para o escopo local do loop 
        # elimina o custo de lookup de atributos do Python (faz muita diferença em milhões de iterações)
        queue = self.events_queue
        pop_event = queue.popleft
        
        calc_signals = self.strategy.calculate_signals
        update_signal = self.portfolio.update_signal
        execute_order = self.execution.execute_order

        # Loop principal pelo feed de dados externos (TICKs)
        for tick in feed_simulado:
            queue.append(tick)

            # Loop Interno Otimizado: Processa a cascata de eventos gerada por esse TICK
            while queue:
                event: Event = pop_event()

                if isinstance(event, TickEvent):
                    calc_signals(event)

                elif isinstance(event, SignalEvent):
                    update_signal(event)

                elif isinstance(event, OrderEvent):
                    execute_order(event)

                elif isinstance(event, FillEvent):
                    print(f"✅ [FILL] Executado {event.quantity} de {event.symbol} a ${event.fill_cost} | Taxa: ${event.commission:.4f}")

if __name__ == "__main__":
    # Criando dados de ticks arbitrários e de diferentes classes de ativos
    dados_de_mercado = [
        TickEvent(datetime.now(), "PETR4.SA", 36.20, 36.22),
        TickEvent(datetime.now(), "BTCUSDT", 67250.00, 67251.50),
        TickEvent(datetime.now(), "AAPL", 185.30, 185.35),
        TickEvent(datetime.now(), "ETHUSDT", 3510.25, 3510.80)
    ]

    # Instancia a Engine única
    engine = TradingEngine()
    
    # Roda o backtest/feed
    start_time = time.perf_counter()
    engine.run(dados_de_mercado)
    end_time = time.perf_counter()
    
    print(f"\n⚡ Loop executado em {(end_time - start_time) * 1000:.4f} milissegundos.")

import pandas as pd
import numpy as np

class GeradorFeaturesMorfologicas:
    """
    Gera recursos estatísticos avançados para qualquer série temporal de preços,
    de forma vetorizada e independente do tipo de ativo.
    """
    def __init__(self, janela_curta: int = 20, janela_longa: int = 50):
        self.j_curta = janela_curta
        self.j_longa = janela_longa

    def construir_matriz(self, df_padrao: pd.DataFrame) -> pd.DataFrame:
        """
        Recebe o DataFrame padronizado e injeta as colunas de recursos.
        """
        # Evita modificar o DataFrame original
        df = df_padrao.copy().sort_values('Data')
        
        # 1. RETORNOS (Base para cálculos de volatilidade estrutural)
        # Usamos retornos logarítmicos por estabilidade estatística
        df['Retornos_Log'] = np.log(df['Fechamento'] / df['Fechamento'].shift(1))

        # ==========================================
        # VOLATILIDADE MÓVEL
        # ==========================================
        # Volatilidade móvel diária/minuto baseada no desvio padrão dos retornos
        df['Volatilidade_Curta'] = df['Retornos_Log'].rolling(window=self.j_curta).std()
        df['Volatilidade_Longa'] = df['Retornos_Log'].rolling(window=self.j_longa).std()
        
        # Razão de volatilidade (identifica expansão ou compressão de volatilidade)
        df['Razao_Volatilidade'] = df['Volatilidade_Curta'] / (df['Volatilidade_Longa'] + 1e-9)

        # ==========================================
        # DIFERENCIAIS ESTATÍSTICOS (Z-SCORE)
        # ==========================================
        # Média e desvio padrão do PREÇO para cálculo de distanciamento
        media_movel = df['Fechamento'].rolling(window=self.j_curta).mean()
        desvio_movel = df['Fechamento'].rolling(window=self.j_curta).std()
        
        # Z-Score: diz se o preço está "caro" ou "barato" estatisticamente isolado
        df['Z_Score_Preco'] = (df['Fechamento'] - media_movel) / (desvio_movel + 1e-9)

        # ==========================================
        # ASSIMETRIA DE PREÇOS (SKEWNESS)
        # ==========================================
        # Mede a força das caudas de distribuição dos preços (anomalias de preço)
        df['Assimetria_Curta'] = df['Fechamento'].rolling(window=self.j_curta).skew()
        df['Assimetria_Longa'] = df['Fechamento'].rolling(window=self.j_longa).skew()

        # ==========================================
        # LIMPEZA DA MATRIZ
        # ==========================================
        # Remove as linhas iniciais que ficam com NaN devido às janelas móveis
        df_matriz = df.dropna()
        
        return df_matriz

if __name__ == "__main__":
    # Simulando um DataFrame que viria do seu Extrator Abstrato
    np.random.seed(42)
    datas = pd.date_range(start="2026-01-01", periods=100, freq="D")
    
    # Criando uma caminhada aleatória (Random Walk) para simular preços reais
    precos_simulados = 100 + np.cumsum(np.random.normal(0, 1.5, size=100))
    
    df_mercado = pd.DataFrame({
        "Data": datas,
        "Fechamento": precos_simulados
    })

    # Instanciando o Engenheiro de Features
    pipeline_features = GeradorFeaturesMorfologicas(janela_curta=10, janela_longa=30)
    
    # Transformando os dados brutos em Matriz Matemática
    matriz_pronta = pipeline_features.construir_matriz(df_mercado)
    
    # Visualizando a Matriz de Features Estruturada
    colunas_relevantes = [
        'Data', 'Fechamento', 'Volatilidade_Curta', 
        'Z_Score_Preco', 'Assimetria_Curta'
    ]
    
    print("📋 MATRIZ DE FEATURES (Primeiras Linhas Processadas):")
    print(matriz_pronta[colunas_relevantes].head(5).to_string(index=False))

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

class TreinadorFiltroIA:
    def __init__(self, features_df: pd.DataFrame):
        self.df = features_df.copy().sort_values('Data')
        self.modelo = None

    def preparar_targets(self, horizonte_previsao: int = 5) -> pd.DataFrame:
        """
        Cria as regras de rotulação (Target).
        Horizonte: número de períodos à frente para julgar se a compra foi boa.
        """
        # Simulação de um sinal da estratégia básica: compra quando Z-Score está abaixo de -1.5
        self.df['Sinal_Estrategia'] = np.where(self.df['Z_Score_Preco'] < -1.5, 1, 0)
        
        # Retorno futuro após o sinal de compra
        self.df['Retorno_Futuro'] = self.df['Fechamento'].shift(-horizonte_previsao) / self.df['Fechamento'] - 1
        
        # Gerando o Target (Meta-Label): 
        # 1 = Deu sinal E o preço subiu no futuro (Sucesso)
        # 0 = Deu sinal E o preço caiu/ficou estável, OU não deu sinal.
        self.df['Target'] = np.where((self.df['Sinal_Estrategia'] == 1) & (self.df['Retorno_Futuro'] > 0.005), 1, 0)
        
        # Remove dados do final do DataFrame que não possuem histórico futuro suficiente
        return self.df.dropna()

    def treinar(self):
        # 1. Preparação dos dados com os alvos calculados
        df_processado = self.preparar_targets()
        
        # Selecionando as colunas que a IA vai usar para decidir (recursos que criamos antes)
        features_colunas = [
            'Volatilidade_Curta', 'Volatilidade_Longa', 'Razao_Volatilidade', 
            'Z_Score_Preco', 'Assimetria_Curta', 'Assimetria_Longa'
        ]
        
        X = df_processado[features_colunas]
        y = df_processado['Target']
        
        # 2. Divisão de Treino e Teste sem embaralhar (Time Series Split conceitual)
        # Em séries temporais NÃO podemos usar shuffle=True para não vazar dados do futuro no passado
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False)
        
        print(f"📊 Tamanho da base de Treino: {len(X_train)} | Teste: {len(X_test)}")
        print(f"⚖️ Distribuição das classes no treino: \n{y_train.value_counts(normalize=True)}")
        
        # 3. Configuração do LightGBM Classificador
        # Lidamos com o desbalanceamento de classes usando 'is_unbalance=True'
        self.modelo = lgb.LGBMClassifier(
            objective='binary',
            n_estimators=100,
            learning_rate=0.05,
            max_depth=5,
            is_unbalance=True,
            random_state=42,
            verbosity=-1
        )
        
        # Treinamento
        self.modelo.fit(X_train, y_train)
        
        # 4. Avaliação do Modelo
        predicoes = self.modelo.predict(X_test)
        
        print("\n🎯 --- RELATÓRIO DE PERFORMANCE (BASE DE TESTE) ---")
        print(classification_report(y_test, predicoes, target_names=['Bloquear (0)', 'Validar (1)']))
        
        print("🧩 --- MATRIZ DE CONFUSÃO ---")
        print(confusion_matrix(y_test, predicoes))
        
        return self.modelo

    def classificar_sinal_tempo_real(self, nova_linha_features: dict) -> int:
        """
        Recebe um dicionário com a linha atual gerada pela Engine de Eventos,
        e decide se autoriza (1) ou bloqueia (0) a ordem.
        """
        if self.modelo is None:
            raise ValueError("O modelo precisa ser treinado antes de classificar.")
            
        # Converte a linha de entrada no formato esperado pelo modelo
        df_linha = pd.DataFrame([nova_linha_features])
        features_colunas = [
            'Volatilidade_Curta', 'Volatilidade_Longa', 'Razao_Volatilidade', 
            'Z_Score_Preco', 'Assimetria_Curta', 'Assimetria_Longa'
        ]
        
        predicao = self.modelo.predict(df_linha[features_colunas])
        return int(predicao[0])

class StrategyComFiltroIA:
    def __init__(self, events_queue, modelo_ia_treinado):
        self.events_queue = events_queue
        self.ia = modelo_ia_treinado

    def calculate_signals(self, tick, features_do_tick: dict):
        """
        features_do_tick: dicionário contendo as métricas calculadas em tempo real 
        (Z-Score, Volatilidade, Assimetria) para o candle atual.
        """
        # 1. Regra matemática básica dispara um alerta interno
        if features_do_tick['Z_Score_Preco'] < -1.5: 
            
            # 2. A IA avalia o cenário macro e decide
            autorizado_pela_ia = self.ia.classificar_sinal_tempo_real(features_do_tick)
            
            if autorizado_pela_ia == 1:
                print(f"🤖 [IA - AUTORIZADO] Padrão estatístico favorável para {tick.symbol}. Enviando sinal.")
                signal = SignalEvent(symbol=tick.symbol, direction="BUY", strength=1.0)
                self.events_queue.append(signal)
            else:
                print(f"🛡️ [IA - BLOQUEADO] Sinal matemático ignorado. Risco de falso rompimento detectado.")

import numpy as np

class PortfolioComKelly:
    """
    Gerencia o capital da conta, calcula o tamanho do lote dinamicamente
    usando o Critério de Kelly (Fracionado) e gera as ordens.
    """
    def __init__(self, events_queue, capital_inicial: float, fraction: float = 0.5):
        self.events_queue = events_queue
        self.capital_disponivel = capital_inicial
        self.fraction = fraction  # Ex: 0.5 = Half Kelly (Mais seguro)
        
        # Métricas históricas de performance da estratégia (atualizadas pós-backtest ou tracking)
        # Podem ser calculadas globalmente ou por ativo.
        self.win_rate = 0.55         # 55% de acerto
        self.risk_reward_ratio = 1.8 # Ganha $1.80 para cada $1.00 perdido

    def calcular_fracao_kelly(self) -> float:
        """Calcula a porcentagem teórica do capital a ser arriscada."""
        W = self.win_rate
        R = self.risk_reward_ratio
        
        if R <= 0:
            return 0.0
            
        # Fórmula matemática de Kelly
        kelly_f = W - ((1 - W) / R)
        
        # Se Kelly for negativo, a estratégia tem expectativa matemática negativa (Não operar!)
        return max(0.0, kelly_f)

    def update_signal(self, signal, preco_atual: float):
        """
        Consome os sinais validados e calcula o tamanho exato da ordem (lote).
        """
        # 1. Obtém a fração de Kelly sugerida e aplica a redução fracionária de risco
        f_kelly = self.calcular_fracao_kelly()
        porcentagem_risco = f_kelly * self.fraction
        
        # Capital financeiro total que podemos alocar nesta operação
        capital_alocado = self.capital_disponivel * porcentagem_risco
        
        # 2. Calcula o tamanho do lote com base no preço nominal do ativo (Ação ou Cripto)
        if preco_atual <= 0:
            print(f"⚠️ [PORTFÓLIO] Preço inválido para {signal.symbol}. Ordem abortada.")
            return

        quantidade_lote = capital_alocado / preco_atual
        
        # Regras de arredondamento dependendo do mercado
        if "USDT" in signal.symbol:
            # Cripto: Permite frações decimais (Ex: 0.005 BTC)
            quantidade_lote = round(quantidade_lote, 4)
        else:
            # Ações tradicionais (Ex: B3 ou Mercado Americano sem fracionado): Lotes inteiros
            quantidade_lote = int(np.floor(quantidade_lote))

        # 3. Dispara a ordem se o lote mínimo for viável
        if quantidade_lote > 0:
            print(f"💰 [KELLY] Capital Total: ${self.capital_disponivel:.2f} | Risco: {porcentagem_risco*100:.1f}% | Alocado: ${capital_alocado:.2f}")
            
            order = OrderEvent(
                symbol=signal.symbol,
                order_type="MARKET",
                quantity=quantidade_lote,
                direction=signal.direction
            )
            self.events_queue.append(order)
        else:
            print(f"⚠️ [PORTFÓLIO] Capital alocado insuficiente para comprar 1 unidade mínima de {signal.symbol}.")

    def update_fill(self, fill):
        """Atualiza o saldo real disponível após a execução da ordem no mercado."""
        custo_total = (fill.quantity * fill.fill_cost) + fill.commission
        
        if fill.direction == "BUY":
            self.capital_disponivel -= custo_total
            print(f"💳 [CONTA] Posição Aberta. Saldo Atualizado: ${self.capital_disponivel:.2f}")
        elif fill.direction == "SELL":
            # Em caso de venda, o capital retorna somado/subtraído do resultado (simplificado)
            self.capital_disponivel += custo_total 
            print(f"💳 [CONTA] Posição Fechada. Saldo Atualizado: ${self.capital_disponivel:.2f}")

if __name__ == "__main__":
    from collections import deque
    from typing import NamedTuple
    
    # Mock dos eventos mínimos necessários para o teste
    class SignalEvent(NamedTuple): symbol: str; direction: str
    class OrderEvent(NamedTuple): symbol: str; order_type: str; quantity: float; direction: str

    fila_eventos = deque()
    
    # Iniciando a conta com $10.000 USD
    portfolio = PortfolioComKelly(events_queue=fila_eventos, capital_inicial=10000.0, fraction=0.5)
    
    # Cenário 1: Sinal para Bitcoin (Preço alto, lote fracionado)
    sinal_btc = SignalEvent(symbol="BTCUSDT", direction="BUY")
    print("\n--- Processando Sinal 1 ---")
    portfolio.update_signal(sinal_btc, preco_atual=65000.00)
    
    # Cenário 2: Sinal para uma Ação Nacional (Preço baixo, lote inteiro)
    sinal_petr = SignalEvent(symbol="PETR4.SA", direction="BUY")
    print("\n--- Processando Sinal 2 ---")
    portfolio.update_signal(sinal_petr, preco_atual=36.50)

    # Imprime o que foi gerado na fila de saída para a ExecutionHandler
    print("\n📦 Ordens geradas na Fila de Eventos:")
    while fila_eventos:
        print(fila_eventos.popleft())


