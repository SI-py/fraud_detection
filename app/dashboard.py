import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json
import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st
from app.config import DATABASE_URL
from app.preprocessing import preprocess
from scripts.produce import send_frame

st.set_page_config(page_title='Скоринг транзакций', layout='wide', initial_sidebar_state='expanded')
st.markdown("""<style>
.block-container {padding-top:2rem; max-width:1200px;}
h1 {font-size:1.8rem!important; font-weight:600!important; letter-spacing:0!important;}
h2,h3 {font-weight:600!important;}
[data-testid="stMetricValue"] {font-size:1.6rem;}
[data-testid="stSidebar"] {border-right:1px solid #e2e5e9;}
.stButton>button {border-radius:4px;}
</style>""", unsafe_allow_html=True)
metadata = json.loads(Path('models/metadata.json').read_text())
with st.sidebar:
    st.markdown('### Скоринг транзакций')
    page = st.radio('Раздел', ['Загрузка CSV', 'Результаты', 'Модель'])
    st.divider()
    st.caption('CatBoost / CPU')
    st.caption(f'Порог фрода: {metadata["threshold"]:.3f}')
    st.link_button('Kafka UI', 'http://localhost:8080')

if page == 'Загрузка CSV':
    st.title('Загрузка транзакций')
    st.write('Загрузите CSV файл с транзакциями.')
    upload = st.file_uploader('CSV с транзакциями', type=['csv'])
    frame = None
    if upload:
        try:
            frame = pd.read_csv(upload)
            preprocess(frame)
            st.success(f'Формат проверен · {len(frame):,} транзакций')
            st.dataframe(frame.head(10), use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(f'Не удалось прочитать CSV: {exc}')
            frame = None
    if frame is not None and len(frame):
        limit = st.number_input('Количество строк для отправки', min_value=1, max_value=len(frame), value=min(100, len(frame)))
        if st.button('Отправить в Kafka', type='primary'):
            try:
                with st.spinner('Отправляем транзакции…'):
                    count = send_frame(frame.head(int(limit)))
                st.success(f'Доставлено в Kafka: {count}. Откройте раздел «Результаты» и нажмите «Посмотреть результаты».')
            except Exception as exc:
                st.error(f'Ошибка отправки; часть строк могла быть доставлена: {exc}')
    st.caption('transaction_id добавляется автоматически. Повторная загрузка CSV создаёт новую партию.')

elif page == 'Результаты':
    st.title('Результаты скоринга')
    st.caption('Записи из PostgreSQL. Данные обновляются по кнопке.')
    if st.button('Посмотреть результаты', type='primary'):
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.transaction():
                    conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    totals = conn.execute('SELECT count(*), count(*) FILTER (WHERE fraud_flag=1), avg(score) FROM transaction_scores').fetchone()
                    recent = conn.execute('SELECT transaction_id, score, fraud_flag, created_at FROM transaction_scores ORDER BY id DESC LIMIT 100').fetchall()
                    fraud = conn.execute('SELECT transaction_id, score, created_at FROM transaction_scores WHERE fraud_flag=1 ORDER BY id DESC LIMIT 10').fetchall()
            st.session_state['snapshot'] = (totals, recent, fraud, pd.Timestamp.now(tz='Europe/Moscow'))
        except Exception as exc:
            st.error(f'База данных недоступна: {exc}')
    if 'snapshot' not in st.session_state:
        st.info('Нажмите «Посмотреть результаты», чтобы загрузить данные из базы.')
    else:
        totals, recent, fraud, updated = st.session_state['snapshot']
        n, f, avg = totals
        cols = st.columns(4)
        for col, label, value in zip(cols, ['Транзакций', 'С флагом фрода', 'Доля фрода', 'Средний скор'], [f'{n:,}', f'{f:,}', f'{100*f/n:.1f}%' if n else '—', f'{avg:.3f}' if avg is not None else '—']):
            col.metric(label, value)
        st.caption(f'Обновлено: {updated.strftime("%H:%M:%S МСК")}')
        if not n:
            st.info('Пока нет результатов. Загрузите файл в разделе «Загрузка CSV».')
        else:
            data = pd.DataFrame(recent, columns=['transaction_id','score','fraud_flag','created_at'])
            distribution, composition = st.columns([1.6, 1])
            with distribution:
                st.subheader('Распределение скоров')
                st.caption(f'Последние {len(data)} транзакций')
                log_counts = st.checkbox('Логарифмическая шкала количества', value=True)
                fig = px.histogram(data, x='score', template='plotly_white', color_discrete_sequence=['#496a8a'], log_y=log_counts)
                fig.update_traces(xbins=dict(start=0, end=1, size=.025), marker_line_width=0, hovertemplate='Скор: %{x}<br>Транзакций: %{y}<extra></extra>')
                fig.add_vline(x=metadata['threshold'], line_dash='dash', line_color='#a4473c', annotation_text='Порог фрода')
                fig.update_layout(height=340, paper_bgcolor='#ffffff', plot_bgcolor='#ffffff', xaxis_title='Скор модели', yaxis_title='Число транзакций', bargap=.12, margin=dict(l=15,r=15,t=25,b=20), font=dict(color='#27313b'))
                fig.update_xaxes(range=[0,1], showgrid=False)
                fig.update_yaxes(gridcolor='#eceef0')
                if log_counts:
                    import math
                    ticks = [v for v in [1, 2, 5, 10, 20, 50, 100] if v <= len(data)]
                    fig.update_yaxes(range=[math.log10(.7), math.log10(max(2, len(data)*1.2))], tickvals=ticks, ticktext=[str(v) for v in ticks])
                else:
                    fig.update_yaxes(rangemode='tozero')
                st.plotly_chart(fig, use_container_width=True)
            with composition:
                st.subheader('Доля фрода')
                st.caption(f'Все {n:,} транзакций в базе')
                fig = px.pie(names=['Без флага фрода', 'С флагом фрода'], values=[n-f, f], hole=.55, template='plotly_white', color_discrete_sequence=['#496a8a', '#b65c4d'])
                fig.update_traces(textinfo='percent', textposition='outside', hovertemplate='%{label}<br>%{value} транзакций (%{percent})<extra></extra>')
                fig.update_layout(height=390, paper_bgcolor='#ffffff', margin=dict(l=20,r=20,t=40,b=35), legend=dict(orientation='h', y=-.1, x=.5, xanchor='center'), font=dict(color='#27313b'))
                st.plotly_chart(fig, use_container_width=True)
            st.subheader('Транзакции с наибольшим скором')
            st.caption(f'10 наибольших скоров среди последних {len(data)} транзакций')
            top = data.nlargest(10, 'score').sort_values('score')
            top = top.assign(status=top.fraud_flag.map({0:'Без флага фрода', 1:'С флагом фрода'}))
            fig = px.bar(top, x='score', y='transaction_id', orientation='h', color='status', color_discrete_map={'Без флага фрода':'#496a8a', 'С флагом фрода':'#b65c4d'}, template='plotly_white', labels={'score':'Скор модели', 'transaction_id':'Транзакция', 'status':'Результат'})
            fig.update_traces(hovertemplate='%{y}<br>Скор: %{x:.5f}<extra></extra>')
            fig.add_vline(x=metadata['threshold'], line_dash='dash', line_color='#a4473c', annotation_text='Порог фрода')
            fig.update_layout(height=350, paper_bgcolor='#ffffff', plot_bgcolor='#ffffff', margin=dict(l=15,r=15,t=30,b=20), showlegend=False, font=dict(color='#27313b'))
            fig.update_xaxes(range=[0, min(1, max(float(top.score.max()), metadata['threshold'])*1.2)], gridcolor='#eceef0')
            fig.update_yaxes(type='category', categoryorder='array', categoryarray=top.transaction_id.tolist())
            st.plotly_chart(fig, use_container_width=True)
            st.subheader('Последние фродовые транзакции')
            st.caption('10 последних записей с fraud_flag = 1')
            if fraud:
                st.dataframe(pd.DataFrame(fraud, columns=['transaction_id','score','created_at']), hide_index=True, use_container_width=True, column_config={'score': st.column_config.NumberColumn('Скор', format='%.4f')})
            else:
                st.info('Транзакций с флагом фрода пока нет.')
            with st.expander('Последние 100 результатов'):
                st.dataframe(data, use_container_width=True, hide_index=True)
                st.download_button('Скачать результаты CSV', data.to_csv(index=False), 'scores.csv', 'text/csv')
else:
    st.title('Модель')
    st.write(f'CatBoostClassifier: градиентный бустинг над деревьями, {metadata["tree_count"]} деревьев глубины 6. Обучение с Logloss и early stopping по ROC AUC. Inference на CPU.')
    st.caption('19 признаков: сумма, категория, продавец, время транзакции, данные клиента и география. Категориальные признаки обрабатывает CatBoost; числовые и временные признаки готовит общий пайплайн.')
    st.write(f'Скор от 0 до 1: чем выше, тем подозрительнее транзакция. При скоре от {metadata["threshold"]:.3f} ставится флаг фрода.')
    st.subheader('Качество модели')
    st.caption('Хронологический split: 70% train / 15% validation / 15% holdout. Порог выбран по максимуму F1 на validation; ниже — метрики holdout (117 965 транзакций).')
    cols = st.columns(4)
    for col, (key, label) in zip(cols, [('roc_auc','ROC AUC'),('pr_auc','PR AUC'),('precision','Precision'),('recall','Recall')]):
        col.metric(label, f'{metadata["holdout"][key]:.4f}')
    st.caption(f'F1: {metadata["holdout"]["f1"]:.4f}')
    st.subheader('Важность признаков')
    imp = pd.DataFrame(list(metadata['feature_importance'].items())[:12], columns=['Признак','Важность'])
    fig = px.bar(imp.iloc[::-1], x='Важность', y='Признак', orientation='h', template='plotly_white', color_discrete_sequence=['#496a8a'])
    fig.update_layout(height=380, paper_bgcolor='#ffffff', plot_bgcolor='#ffffff', margin=dict(l=15,r=15,t=15,b=20))
    st.plotly_chart(fig, use_container_width=True)
