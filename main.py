import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, round, mean
from datetime import date, time, datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Inizializziamo la sessione Spark
spark = SparkSession.builder.getOrCreate()

# Carichiamo il file CSV
df = spark.read.csv("Log.csv", header=True, inferSchema=True, sep=';')

# Rimozione delle istanze duplicate
df = df.dropDuplicates()

# Ordiniamo le righe in ordine crescente rispetto al campo 'Datetime'
df = df.orderBy(col('Datetime'))

# Conversioni
df = df.withColumn('Datetime', from_unixtime(col('Datetime')).cast('string'))
df = df.withColumn('Sunrise', from_unixtime(col('Sunrise')).cast('string'))
df = df.withColumn('Sunset', from_unixtime(col('Sunset')).cast('string'))
df = df.withColumn('Temperature', round(col('Temperature') - 273.15, 2))
df = df.withColumn('Feels_Like', round(col('Feels_Like') - 273.15, 2))
df = df.withColumn('Temp_Min', round(col('Temp_Min') - 273.15, 2))
df = df.withColumn('Temp_Max', round(col('Temp_Max') - 273.15, 2))

# Dividiamo la colonna 'Datetime' in due nuove colonne denominate 'Date' e 'Time'
df = df.withColumn('Date', df['Datetime'].substr(1, 10))
df = df.withColumn('Time', df['Datetime'].substr(12, 8))

# Dividiamo la colonne 'Sunrise' e 'Sunset' nelle nuove colonne denominate 'Time_Sunrise' e 'Time_Sunset'
df = df.withColumn('Sunrise_Time', df['Sunrise'].substr(12, 8))
df = df.withColumn('Sunset_Time', df['Sunset'].substr(12, 8))

# Eliminazione delle colonne 'Datetime', 'Sunrise' e 'Sunset'
df = df.drop('Datetime')
df = df.drop('Sunrise')
df = df.drop('Sunset')

# Instanziamo delle variabili globali
data_minima = date(2023, 5, 31)
data_massima = date(2023, 6, 2)
min_time = time(0, 0)
max_time = time(23, 59)
min_time_int = 0
max_time_int = 0

st.set_page_config(
    page_title="Dashboard Analitiche Meteo (NA)",
    page_icon="🌤️",
    layout="wide"
)


def elenco_bullet(testo_grassetto, testo_normale):
    st.markdown(f"- <span style='color:#FFA559'><b>{testo_grassetto}</b></span>: {testo_normale}",
                unsafe_allow_html=True)


# Creiamo la barra laterale
with st.sidebar:
    pagina_selezionata = option_menu(
        menu_title="Sezioni",
        options=["Home", "Documentazione", "Analitica 1", "Analitica 2", "Analitica 3"],
    )

if pagina_selezionata == "Home":
    # Immagine di copertina
    st.image('ImmagineCopertinaDashboardMeteoNapoliBordoArancio.jpg', use_column_width=True)

    st.title("HomePage - Dashboard analitiche meteo")
    st.header("Analitiche meteorologiche - Città di Napoli ☀️ 🌦️ ⛈️️")
    st.write("""
      Questo progetto utilizza i dati generati dal sito '*Open Weather*', che offre informazioni meteorologiche
      globali. In particolare, abbiamo ottenuto accesso ai dati mediante l'API '*One Call API 3.0*', grazie alla quale
      abbiamo recuperato le informazioni meteo esclusive della città di Napoli. I dati sono stati raccolti in tempo
      reale attraverso *Kafka*, successivamente elaborati tramite *PySpark* ed, infine, i risultati delle analisi effettuate
      vengono visualizzati in modo intuitivo e interattivo tramite una dashboard *Streamlit*.
      """)

    st.write("""
          Le analitiche effettuate che ritroviamo in questa dashboard sono le seguenti:
          """)

    # Elenco con bullet list e link ai collegamenti delle sezioni
    elenco_bullet("Prima Analitica", "Permette di visualizzare tutte le informazioni relative al meteo di un giorno specifico. "
                                     "Il giorno è selezionabile tramite un apposito form di input. Il risultato dell'analitica viene mostrato attraverso "
                                     "un grafico che riporta l'andamento di una serie di caratteristiche meteorologiche tra cui la temperatura "
                                     "(comprendendo anche minima, massima e percepita), pressione atmosferica, umidità, visibilità, velocità del vento, raffiche di vento, direzione del vento,"
                                     " orario di alba e tramonto e nuvolosità. Inoltre, viene riportata anche una tabella che mostra i valori medi misurati durante il giorno selezionato.")
    elenco_bullet("Seconda Analitica", "Permette di effettuare una serie di confronti tra 3 giorni specifici selezionabili tramite appositi form di input."
                                       " I risultati dell'analitica vengono mostrati tramite diversi grafici. Il primo è un istogramma che permette di evidenziare le medie delle caratteristiche"
                                       " meteorologiche (Temperatura, Temperatura Percepita, Pressione, Umidità, Visibilità, Velocità del Vento, Nuvolosità) dei giorni selezionati in modo da "
                                       "poter effettuare anche un semplice confronto visivo. Vengono riportati, inoltre, una serie di areogrammi (uno per ogni giorno selezionato) che "
                                       "evidenziano (in percentuale) le condizioni meteorologiche verificatisi durante la giornata. Tra tali condizioni rientrano *clear sky*,"
                                       " *few clouds*, *scattered clouds*, *broken clouds*, *shower rain*, *rain*, *thunderstorm*,"
                                       " *snow* e *mist*. Infine, viene effettuato un confronto tra le ore di luce dei due giorni estremi dell'intervallo selezionato.")
    elenco_bullet("Terza Analitica", "Mostra, tramite l'apposita selezione di un giorno specifico, la direzione del vento in un determinato orario della giornata. "
                                     "Il risultato di tale analitica viene mostrato attraverso un apposito grafo detto *Scatter Polar*. Di lato, inoltre, viene anche riportata la media della direzione del vento avuta durante la giornata.")

elif pagina_selezionata == "Documentazione":

    st.header("Documentazione Homework 3 - Big Data")

    # Crea un espandibile per l'introduzione
    with st.expander("**Traccia**"):

        st.write("""*"Tramite Apache Kafka gestire l'acquisizione di uno stream di dati (e.g., messaggi da un web server,
      misure da una rete di sensori, etc.) da una data sorgente (anche simulata) e la successiva memorizzazione in un
      file di log su HDFS. Utilizzando poi uno degli strumenti del primo homework (pyspark, pig o hive) effettuare
      delle query sullo stream di dati mostrandone i risultati su un'apposita dashboard".*""")

    # Crea un espandibile per il preprocessing
    with st.expander("**Tecnologie Utilizzate**"):
        st.write("""
          Al fine di poter realizzare le analitiche riportate in questa dashboard, abbiamo utilizzato innanzitutto il 
          servizio di API di *OpenWeather.org* da cui abbiamo raccolto le informazioni necessarie per la realizzazione del dataset. 
          I dati sono stati estratti in tempo reale tramite *Apache Kafka*, piattaforma open-source di stream processing. 
          Nel momento in cui tali dati vengono raccolti, utilizzando il linguaggio *PySpark* vengono distribuiti in un file di log di un HDFS tramite il framework *Hadoop*. 
          Infine, il file di log ottenuto viene utilizzato per poter realizzare una serie di analitiche il cui risultato viene mostrato tramite quest'apposita dashboard *Streamlit*.
           L'IDE utilizzato è *PyCharm*, il quale permette una semplice installazione e gestione delle librerie *PySpark, Hadoop, Kafka e Streamlit* utilizzate.
          """)

        #Immagine dell 'Architettura
        st.image('GraficoArchitetturaHw3.png', use_column_width=True)

    with st.expander("**Presentazione del dataset e Preprocessing**"):
        st.write("""
        Il dataset su cui vengono realizzate le analitiche raccoglie una serie di informazioni riguardanti il meteo a Napoli. Tale dataset è stato lavorato andando a rimuovere i campi che non fornivano alcun contributo informativo (colonne costituite da tutti valori uguali, colonne costituite da tutti valori nulli e così via).
          Il dataset su cui abbiamo realizzato le analitiche risulta essere caratterizzato dai seguenti campi:
          """)
        elenco_bullet("Weather", "Gruppo di parametri meteorologici (Pioggia, Neve, Condizioni Estreme, ecc.)")
        elenco_bullet("Description", "Condizioni meteorologiche all'interno del campo *Weather*")
        elenco_bullet("Temperature", "Temperatura")
        elenco_bullet("Feels_Like", "Temperatura Percepita")
        elenco_bullet("Temp_Min", "Temperatura Minima")
        elenco_bullet("Temp_Max", "Temperatura Massima")
        elenco_bullet("Pressure", "Pressione atmosferica sul livello del mare")
        elenco_bullet("Humidity", "Umidità")
        elenco_bullet("Visibility", "Visibilità")
        elenco_bullet("Wind_Speed", "Velocità del Vento")
        elenco_bullet("Wind_Gust", "Raffiche di Vento")
        elenco_bullet("Wind_Deg", "Direzione del Vento")
        elenco_bullet("Clouds_Level", "Nuvolosità")
        elenco_bullet("Datetime", "Data e Ora di rilevazione")
        elenco_bullet("Sunrise", "Alba")
        elenco_bullet("Sunset", "Tramonto\n")

        st.write("""
        Notiamo, inoltre, che per quanto riguarda i dati relativi alle temperature (in particolare, i campi *Temperature*, *Feels_Like*, *Temp_Min*, *Temp_Max*) è stata necessaria effettuare un'iniziale conversione da Kelvin a Gradi Celsius.
        Infine, per quanto riguarda tutti i campi che presentavano informazioni relative a data ed ora (*Datetime*, *Sunrise* e *Sunset*) il formato impostato nella API di *Openweather* è il datetime UNIX. Per questo motivo, è stata necessaria, anche in questo caso, una conversione che ha portato alla realizzazione di due nuove colonne specifiche relative a data ed ora.      
        """)

elif pagina_selezionata == "Analitica 1":
    st.title('Analitiche per un giorno specifico')

    st.write('''In questa sezione è possibile esplorare le analitiche relative a una specifica
     giornata. E' possibile visualizzare il grafico temporale per uno specifico campo selezionato ed
     accedere alla tabella dei valori medi per quella giornata.''')

    # aggiungi filtro per selezionare una specifica data
    selected_date = st.date_input('Seleziona una data:', value=data_minima, min_value=data_minima,
                                  max_value=data_massima)

    st.header('Grafico Temporale Meteo')

    # Utilizzo degli alias per visualizzare i valori in italiano su Streamlit
    alias_map = {
        'Temperatura': 'Temperature',
        'Temperatura Percepita': 'Feels_Like',
        'Pressione': 'Pressure',
        'Umidità': 'Humidity',
        'Visibilità': 'Visibility',
        'Velocità del Vento': 'Wind_Speed',
        'Nuvolosità': 'Clouds_Level'
    }

    # Aggiungiamo il filtro per selezionare la variabile da visualizzare
    variabile_selezionata_visualizzata = st.selectbox('Seleziona la variabile da visualizzare:', list(alias_map.keys()))

    variabile_selezionata = alias_map[variabile_selezionata_visualizzata]

    # Filtriamo i dati in base alla selezione di date
    filtered_data = df.filter(df['Date'] == selected_date)

    # Calcoliamo la media del campo 'Temperature' dai dati filtrati
    average_temperature = filtered_data.agg(round(mean('Temperature'), 2)).first()[0]
    average_feels_like = filtered_data.agg(round(mean('Feels_Like'), 2)).first()[0]
    average_pressure = filtered_data.agg(round(mean('Pressure'), 2)).first()[0]
    average_humidity = filtered_data.agg(round(mean('Humidity'), 2)).first()[0]
    average_visibility = filtered_data.agg(round(mean('Visibility'), 2)).first()[0]
    average_wind_speed = filtered_data.agg(round(mean('Wind_Speed'), 2)).first()[0]
    average_clouds_level = filtered_data.agg(round(mean('Clouds_Level'), 2)).first()[0]

    filtered_data_pd = filtered_data.toPandas()

    # Creaimao il grafico temporale
    fig = px.line(filtered_data_pd, x='Time', y=variabile_selezionata)

    # Limitiamo il numero di etichette sull'asse x
    num_labels = 6  # Specifica il numero di etichette desiderato

    if len(filtered_data_pd) > num_labels:
        interval = len(filtered_data_pd) // num_labels
        tick_indices = list(range(0, len(filtered_data_pd), interval))
    else:
        tick_indices = [i for i in range(len(filtered_data_pd))]

    tick_indices = [i for i in tick_indices if i < len(filtered_data_pd)]  # Rimuovi gli indici fuori dai limiti
    tick_labels = [filtered_data_pd['Time'].str[:5].iloc[i] for i in
                   tick_indices]  # Prendi i primi 5 caratteri dell'ora
    # Aggiungi l'ultima etichetta solo se ci sono dati nel dataframe
    if len(filtered_data_pd) > 0:
        last_index = len(filtered_data_pd) - 1
        tick_indices.append(last_index)  # Aggiungi l'indice dell'ultimo valore
        tick_labels.append(filtered_data_pd['Time'].str[:5].iloc[last_index])  # Aggiungi l'etichetta dell'ultimo valore
    fig.update_xaxes(tickmode='array', tickvals=tick_indices, ticktext=tick_labels)

    if variabile_selezionata == 'Temperature' or variabile_selezionata == 'Feels_Like':
        fig.add_trace(go.Scatter(
            x=filtered_data_pd['Time'],
            y=filtered_data_pd['Temp_Max'],
            name='Temperatura Massima',
            mode='lines',
            line=dict(color='red')
        ))
        fig.add_trace(go.Scatter(
            x=filtered_data_pd['Time'],
            y=filtered_data_pd['Temp_Min'],
            name='Temperatura Minima',
            mode='lines',
            line=dict(color='blue')
        ))
        fig.add_trace(go.Scatter(
            x=filtered_data_pd['Time'],
            y=filtered_data_pd['Temp_Max'],
            fill='tonexty',
            fillcolor='rgba(255,150,150,0.07)',
            name='Intervallo Max-Min'
        ))
        fig.update_yaxes(title='Gradi (C°)')

    elif variabile_selezionata == 'Pressure':
        fig.update_yaxes(title='Atmosfere (hPa)')

    elif variabile_selezionata == 'Humidity' or variabile_selezionata == 'Clouds_Level':
        fig.update_yaxes(title='Percentuale (%)')

    elif variabile_selezionata == 'Visibility':
        fig.update_yaxes(title='Metri (m)')

    elif variabile_selezionata == 'Wind_Speed':
        fig.update_yaxes(title='Metri/Secondo (m/s)')

    fig.update_xaxes(title='Ora del Giorno')

    # Mostriamo il grafico
    st.plotly_chart(fig, use_container_width=True)

    # Header Tabella
    st.header("Tabella Valori Medi - Giorno '*" + str(selected_date) + "*'")

    # Creaiamo una lista di dizionari con il nome del campo e il valore corrispondente
    table_data = [
        {'Campo (Misura)': '🌡️ Temperatura (C°)', 'Valore Medio': "{:.2f}".format(average_temperature)},
        {'Campo (Misura)': '🥵 Temperatura Percepita (C°)', 'Valore Medio': "{:.2f}".format(average_feels_like)},
        {'Campo (Misura)': '🌫️ Pressione (hPa)', 'Valore Medio': "{:.2f}".format(average_pressure)},
        {'Campo (Misura)': '💧 Umidità (%)', 'Valore Medio': "{:.2f}".format(average_humidity)},
        {'Campo (Misura)': '️😶‍🌫️ Visibilità (m)', 'Valore Medio': "{:.2f}".format(average_visibility)},
        {'Campo (Misura)': '🌬️ Velocità del Vento (m/s)', 'Valore Medio': "{:.2f}".format(average_wind_speed)},
        {'Campo (Misura)': '☁️ Nuvolosità (%)', 'Valore Medio': "{:.2f}".format(average_clouds_level)},
    ]

    # Creaiamo un DataFrame con il dizionario
    df_table = pd.DataFrame(table_data)

    # Aggiungiamo l'header personalizzato
    header = {'Campo (Misura)': 'Campo (Misura)', 'Valore Medio': 'Valore Medio'}

    # Visualizziamo la tabella con gli header personalizzati
    st.write(df_table.rename(columns=header).set_index('Campo (Misura)'))

elif pagina_selezionata == "Analitica 2":

    st.title('Analitiche di confronto tra giorni diversi')
    st.write('''Questa sezione è dedicata alle analisi comparative tra giorni differenti, in particolare
      è possibile confrontare i valori medi dei campi e visualizzare grafici e tabelle che evidenziano le differenze
      tra i vari giorni.''')

    # Layout a tre colonne
    col1, col2, col3 = st.columns(3)

    with col1:
        # Aggiungiamo filtro per selezionare una specifica data
        day_1 = st.date_input('Seleziona la data del primo giorno:', value=data_minima, min_value=data_minima,
                              max_value=data_massima, key='date_input_1')

    with col2:
        # Aggiungiamo filtro per selezionare una specifica data
        day_2 = st.date_input('Seleziona la data del secondo giorno:', value=date(2023, 6, 1), min_value=data_minima,
                              max_value=data_massima, key='date_input_2')

    with col3:
        # Aggiungiamo filtro per selezionare una specifica data
        day_3 = st.date_input('Seleziona la data del terzo giorno:', value=date(2023, 6, 2), min_value=data_minima,
                              max_value=data_massima, key='date_input_3')

    st.header('Confronto tra valori medi giornalieri dei campi')

    # Utilizziamo degli alias per visualizzare i valori in italiano su Streamlit
    alias_map = {
        'Temperatura': 'Temperature',
        'Temperatura Percepita': 'Feels_Like',
        'Pressione': 'Pressure',
        'Umidità': 'Humidity',
        'Visibilità': 'Visibility',
        'Velocità del Vento': 'Wind_Speed',
        'Nuvolosità': 'Clouds_Level'
    }

    # Aggiungiamo il filtro per selezionare la variabile da visualizzare
    variabile_selezionata_visualizzata = st.selectbox('Seleziona la variabile da visualizzare:', list(alias_map.keys()))
    variabile_selezionata = alias_map[variabile_selezionata_visualizzata]

    temperature_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Temperature'), 2)).first()[0]
    temperature_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Temperature'), 2)).first()[0]
    temperature_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Temperature'), 2)).first()[0]
    feels_like_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Feels_Like'), 2)).first()[0]
    feels_like_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Feels_Like'), 2)).first()[0]
    feels_like_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Feels_Like'), 2)).first()[0]
    pressure_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Pressure'), 2)).first()[0]
    pressure_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Pressure'), 2)).first()[0]
    pressure_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Pressure'), 2)).first()[0]
    humidity_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Humidity'), 2)).first()[0]
    humidity_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Humidity'), 2)).first()[0]
    humidity_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Humidity'), 2)).first()[0]
    visibility_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Visibility'), 2)).first()[0]
    visibility_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Visibility'), 2)).first()[0]
    visibility_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Visibility'), 2)).first()[0]
    wind_speed_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Wind_Speed'), 2)).first()[0]
    wind_speed_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Wind_Speed'), 2)).first()[0]
    wind_speed_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Wind_Speed'), 2)).first()[0]
    clouds_level_day_one = df.filter(df['Date'] == day_1).agg(round(mean('Clouds_Level'), 2)).first()[0]
    clouds_level_day_two = df.filter(df['Date'] == day_2).agg(round(mean('Clouds_Level'), 2)).first()[0]
    clouds_level_day_three = df.filter(df['Date'] == day_3).agg(round(mean('Clouds_Level'), 2)).first()[0]

    #Formattiamo i giorni in formato stringa
    day_1_str = day_1.strftime("%d/%m/%Y")
    day_2_str = day_2.strftime("%d/%m/%Y")
    day_3_str = day_3.strftime("%d/%m/%Y")

    # Creiamo una lista di dizionari
    data = [
        {'Giorno': day_1_str, 'Temperature': temperature_day_one, 'Feels_Like': feels_like_day_one,
         'Pressure': pressure_day_one, 'Humidity': humidity_day_one, 'Visibility': visibility_day_one,
         'Wind_Speed': wind_speed_day_one, 'Clouds_Level': clouds_level_day_one},
        {'Giorno': day_2_str, 'Temperature': temperature_day_two, 'Feels_Like': feels_like_day_two,
         'Pressure': pressure_day_two, 'Humidity': humidity_day_two, 'Visibility': visibility_day_two,
         'Wind_Speed': wind_speed_day_two, 'Clouds_Level': clouds_level_day_two},
        {'Giorno': day_3_str, 'Temperature': temperature_day_three, 'Feels_Like': feels_like_day_three,
         'Pressure': pressure_day_three, 'Humidity': humidity_day_three, 'Visibility': visibility_day_three,
         'Wind_Speed': wind_speed_day_three, 'Clouds_Level': clouds_level_day_three}
    ]

    # Calcoliamo il valore massimo per l'asse y
    max_value = max([item[variabile_selezionata] for item in data])
    y_max = max_value + max_value * 0.25  # Imposta il valore massimo dell'asse y al 10% del valore massimo dei dati

    # Creazione dell'istogramma
    fig = go.Figure(
        data=[go.Bar(x=[item['Giorno'] for item in data], y=[item[variabile_selezionata] for item in data],width=0.3)])

    if variabile_selezionata == 'Temperature':
        fig.update_yaxes(title='Temperatura (°C)')
        fig.update_layout(
            title='Variazione Temperatura',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Feels_Like':
        fig.update_yaxes(title='Temperatura (°C)')
        fig.update_layout(
            title='Variazione Temperatura Percepita',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Pressure':
        fig.update_yaxes(title='Atmosfere (hPa)')
        fig.update_layout(
            title='Variazione Pressione',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Humidity':
        fig.update_yaxes(title='Percentuale (%)')
        fig.update_layout(
            title='Variazione Umidità',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Clouds_Level':
        fig.update_yaxes(title='Percentuale (%)')
        fig.update_layout(
            title='Variazione Nuvolosità',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Visibility':
        fig.update_yaxes(title='Metri (m)')
        fig.update_layout(
            title='Variazione Visibilità',
            yaxis_range=[0, y_max]
        )

    elif variabile_selezionata == 'Wind_Speed':
        fig.update_yaxes(title='Metri/Secondo (m/s)')
        fig.update_layout(
            title='Variazione Velocità del Vento',
            yaxis_range=[0, y_max]
        )

    fig.update_xaxes(title='Giorno')
    fig.update_traces(marker_color='#FFA559')

    # Visualizzazione dell'istogramma in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    # AEROGRAMMI

    st.header('Confronto condizioni meteorologiche')

    # Calcoliamo il conteggio dei valori nella colonna 'Description'
    description_counts_1 = df.filter(df['Date'] == day_1).groupby('Description').count().select('Description',
                                                                                                'count').toPandas()
    # Calcoliamo il conteggio dei valori nella colonna 'Description'
    description_counts_2 = df.filter(df['Date'] == day_2).groupby('Description').count().select('Description',
                                                                                                'count').toPandas()
    # Calcoliamo il conteggio dei valori nella colonna 'Description'
    description_counts_3 = df.filter(df['Date'] == day_3).groupby('Description').count().select('Description',
                                                                                                'count').toPandas()

    # Creiamo un'unica legenda per tutti i grafici
    legend_labels = description_counts_1['Description'].tolist() + description_counts_2['Description'].tolist() + \
                    description_counts_3['Description'].tolist()

    # Creiamo la figura con tre sottoplot di tipo pie
    fig = make_subplots(rows=1, cols=3, specs=[[{'type': 'pie'}, {'type': 'pie'}, {'type': 'pie'}]],
                        subplot_titles=(day_1_str, day_2_str, day_3_str))

    # Aggiungiamo i grafici torta ai sottoplot
    fig.add_trace(go.Pie(labels=description_counts_1['Description'], values=description_counts_1['count']), row=1,
                  col=1)
    fig.add_trace(go.Pie(labels=description_counts_2['Description'], values=description_counts_2['count']), row=1,
                  col=2)
    fig.add_trace(go.Pie(labels=description_counts_3['Description'], values=description_counts_3['count']), row=1,
                  col=3)

    # Impostiamo il titolo comune per la legenda
    fig.update_layout(legend=dict(title="Description"))

    # Visualizziamo i grafici in Streamlit
    st.plotly_chart(fig, use_container_width=True)

    st.header("Differenza ore di luce tra il '" + str(day_1) + "' e il '" + str(day_3) + "'")

    row_first_day = df.filter(df['Date'] == day_1).select("Sunrise_Time", "Sunset_Time").collect()[0]
    sunrise_first_day = row_first_day["Sunrise_Time"]
    sunset_first_day = row_first_day["Sunset_Time"]

    row_second_day = df.filter(df['Date'] == day_3).select("Sunrise_Time", "Sunset_Time").collect()[0]
    sunrise_second_day = row_second_day["Sunrise_Time"]
    sunset_second_day = row_second_day["Sunset_Time"]

    # Convertiamo le stringhe in oggetti datetime
    datetime_sunrise_first_day = datetime.strptime(sunrise_first_day, "%H:%M:%S")
    datetime_sunset_first_day = datetime.strptime(sunset_first_day, "%H:%M:%S")

    # Calcoliamo la differenza tra i due tempi
    diff_first_day = datetime_sunset_first_day - datetime_sunrise_first_day

    # Convertiamo le stringhe in oggetti datetime
    datetime_sunrise_second_day = datetime.strptime(sunrise_second_day, "%H:%M:%S")
    datetime_sunset_second_day = datetime.strptime(sunset_second_day, "%H:%M:%S")

    # Calcoliamo la differenza tra i due tempi
    diff_second_day = datetime_sunset_second_day - datetime_sunrise_second_day

    diff_days = str(abs(diff_second_day - diff_first_day))
    diff_first_day = str(diff_first_day)
    diff_second_day = str(diff_second_day)

    # Creiamo una lista di dizionari con il nome del campo e il valore corrispondente
    table_data = [
        {'Fasi del Giorno': 'Alba', 'Valori del ' + str(day_1): sunrise_first_day,
         'Valore del ' + str(day_3): sunrise_second_day},
        {'Fasi del Giorno': 'Tramonto', 'Valori del ' + str(day_1): sunset_first_day,
         'Valore del ' + str(day_3): sunset_second_day},
        {'Fasi del Giorno': 'Ore di Sole', 'Valori del ' + str(day_1): diff_first_day,
         'Valore del ' + str(day_3): diff_second_day}
    ]

    df_table = pd.DataFrame(table_data)

    # Aggiungiamo l'header personalizzato
    header = {'Fasi del Giorno': 'Fasi del Giorno', 'Valori Medio': 'Valori Medio'}

    col1, col2 = st.columns(2)

    # Nella colonna 1, visualizziamo il grafico
    with col1:
        # Visualizziamo la tabella con gli header personalizzati
        st.write(df_table.rename(columns=header).set_index('Fasi del Giorno'))

    # Nella colonna 2, visualizziamo la nota
    with col2:
        st.markdown(
            f'<div style="background-color: #FFA559; padding: 15px; border-radius: 5px;width: 80%;">'
            f'<p style="color: white;"><b>Differenza ore di luce: </b>Nella tabella vengono riportati gli orari di alba e tramonto per i giorni selezionati, mettendo in evidenza un confronto tra le ore di luce totali. La variazione in termini di ore, minuti e secondi registrata è di {diff_days}.</p>'
            f'</div>',
            unsafe_allow_html=True
        )

elif pagina_selezionata == "Analitica 3":

    st.title("Analitica sul Vento")

    st.write('''All'interno di questa sezione, viene presentata un'analisi sulla direzione del vento 
    per un determinato giorno, fornendo informazioni sulla direzione del vento. 
    Nello specifico, l'analisi si concentra sulla distribuzione delle direzioni del vento registrate, 
    offrendo una visione del comportamento del vento durante il giorno preso in considerazione.''')

    cardinal_points = {
        "Nord": 0,
        "Nord-Est": 45,
        "Est": 90,
        "Sud-Est": 135,
        "Sud": 180,
        "Sud-Ovest": 225,
        "Ovest": 270,
        "Nord-Ovest": 315
    }

    def punto_cardinale(angolo):
        differenze = {punto: abs(angolo - valore) for punto, valore in cardinal_points.items()}
        punto_più_vicino = min(differenze, key=differenze.get)
        return punto_più_vicino


    selected_day = st.date_input('Seleziona una data:', value=data_minima, min_value=data_minima,
                                 max_value=data_massima)
    row_selected_day = df.filter(df['Date'] == selected_day).select("Wind_Deg", "Time")
    wind_deg_values = [row["Wind_Deg"] for row in row_selected_day.collect()]
    wind_deg = row_selected_day.agg(round(mean('Wind_Deg'), 1)).first()[0]
    time_values = [row["Time"] for row in row_selected_day.collect()]
    r = list(range(1, len(wind_deg_values) + 1))

    fig = go.Figure(data=go.Scatterpolar(
        theta=wind_deg_values,
        r=time_values,
        mode='markers',
        marker=dict(
            symbol='circle',
            size=10,
            color='blue'
        ),
        showlegend=False
    ))

    fig.update_layout(
        title='Grafico Scatter Polar',
        polar=dict(
            radialaxis=dict(
                visible=True,
                tickfont=dict(color='rgba(0, 0, 0, 0)'),
            )
        )
    )

    # Imposta la larghezza delle colonne
    col1, col2 = st.columns([2, 1])

    # Nella colonna 1, visualizziamo il grafico
    with col1:
        st.plotly_chart(fig)

    # Nella colonna 2, visualizziamo la nota
    with col2:
        st.markdown(
            f'<br><br><br><br><div style="background-color: #FFA559; padding: 15px; border-radius: 5px;width: 60%;">'
            f'<p style="color: white;"><b>Interpretazione dei valori del grafico: </b>Durante la giornata del {selected_day}, il vento ha puntato prevalentemente verso {punto_cardinale(wind_deg)} 🧭</p>'
            f'</div>',
            unsafe_allow_html=True
        )
