import pandas as pd
import json
import requests
import datetime
from pytz import timezone
import xml.etree.ElementTree as ET
import numpy as np


def select_now_time_info(**kwargs): ## 파일명에 쓸있도록 날짜, 시간 가져오기
    code_start_time=datetime.datetime.now(timezone('Asia/Seoul')).strftime('%Y%m%d_%H%M%S') ## 파일명에 사용
    return code_start_time


def get_location_api(api_key,input_nx,input_ny,input_location,**kwargs):
    ## api로 정보 수집
    ## 데이터 저장
    # code_start_time=datetime.datetime.now(timezone('Asia/Seoul')).strftime('%Y%m%d_%H%M%S') ## 파일명에 사용
    code_start_time = kwargs['task_instance'].xcom_pull(task_ids='select_now_time_info')
    
    date_info=datetime.datetime.now(timezone('Asia/Seoul'))
    now_date=date_info.strftime('%Y%m%d') ## 날짜
    if date_info.strftime('%H')=='06': ## 시간대가 오전 6시면
        now_hour='0630' ## 6시30분에 api에 업데이트된 정보 가져오기 -> 오전 7시꺼 가져옴
    elif date_info.strftime('%H')=='17': ## 시간대가 오후 5시면 ## 일단 테스트 용으로
        now_hour='1730' ## 오후5시30분에 api에 있는 정보 가져오기 -> 오후6시꺼 가져오기 가능
    else: ## 연습용
        now_hour='0000'#'1800'
        
    api_key_decode = requests.utils.unquote(api_key)

    params={'serviceKey':api_key_decode,
            'pageNo':1,
            'numOfRows':1000,
            'dataType':"JSON",
            'base_date':now_date, ## 날짜 입력받기
            'base_time':now_hour, ## 시간 입력받기
            'nx':input_nx, ## 시간대에 따른 위치 입력받기
            'ny':input_ny} ## 시간대에 따른 위치 입력받기

    response=requests.get('http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst',params=params)
    contents=response.text
    weather_json=json.loads(contents)
    weather_json_items=weather_json['response']['body']['items']
    weather_df=pd.DataFrame(weather_json_items['item'])
    weather_df.to_csv('files/{0}_data/{0}_data_{1}.csv'.format(input_location,code_start_time),encoding='utf-8',index=False)
    
    return weather_df


def data_transform_result(**kwargs):
    ## 데이터2개 가져오기
    code_start_time = kwargs['task_instance'].xcom_pull(task_ids='select_now_time_info')
    seoul_data=pd.read_csv('files/seoul_data/seoul_data_{0}.csv'.format(code_start_time))
    buchon_data=pd.read_csv('files/seoul_data/seoul_data_{0}.csv'.format(code_start_time))
    
    date_info=datetime.datetime.now(timezone('Asia/Seoul'))
    
    if date_info.strftime('%H')=='06': ## 시간대가 오전 6시면
        seoul_time=['0800','0900']
        buchon_time=['0700']
        # work_or_home='출근'
    
    elif date_info.strftime('%H')=='17': ## 시간대가 오후 5시면 ## 일단 테스트 용으로
        seoul_time=['1800']
        buchon_time=['1900','2000']
        # work_or_home='퇴근'
    else: ## 확인용도로 나중에제거할예정
        # seoul_time=['1800','1900','2000']
        # buchon_time=['1800','1900','2000']
        seoul_time=['0000','0100','0200']
        buchon_time=['0000','0100','0200']
        # work_or_home='퇴근'
        
    
    ## 서울 데이터 피벗
    seoul_pivot=seoul_data.pivot(index=['fcstDate','fcstTime','nx','ny'], columns='category', values='fcstValue') ## 피벗으로
    seoul_pivot.reset_index(drop=False,inplace=True) ## 인덱스 초기화해서 데이터프레임 형식에 맞게 변경
    seoul_pivot.columns=['예측일자','예측시간','X좌표','Y좌표','낙뢰','강수형태','습도','1시간강수량',
                            '하늘상태','기온','동서바람성분','남북바람성분','풍향','풍속']
    seoul_pivot['예측시간']=seoul_pivot['예측시간'].apply(lambda x : str(x))
    seoul_pivot['예측시간']=seoul_pivot['예측시간'].apply(lambda x : x.zfill(4)) ## 4자리로 고정하기
    seoul_pivot['지역']='서울'
    seoul_pivot=seoul_pivot[seoul_pivot['예측시간'].isin(seoul_time)]

    ## 부천 데이터 피벗
    buchon_pivot=buchon_data.pivot(index=['fcstDate','fcstTime','nx','ny'], columns='category', values='fcstValue') ## 피벗으로
    buchon_pivot.reset_index(drop=False,inplace=True) ## 인덱스 초기화해서 데이터프레임 형식에 맞게 변경
    buchon_pivot.columns=['예측일자','예측시간','X좌표','Y좌표','낙뢰','강수형태','습도','1시간강수량',
                            '하늘상태','기온','동서바람성분','남북바람성분','풍향','풍속']
    buchon_pivot['예측시간']=buchon_pivot['예측시간'].apply(lambda x : str(x))
    buchon_pivot['예측시간']=buchon_pivot['예측시간'].apply(lambda x : x.zfill(4)) ## 4자리로 고정하기
    buchon_pivot['지역']='부천'
    buchon_pivot=buchon_pivot[buchon_pivot['예측시간'].isin(buchon_time)]
        
    result_pivot = pd.concat([seoul_pivot,buchon_pivot])
    result_pivot.reset_index(drop=True,inplace=True)
    result_pivot.to_csv('files/result_data/result_{0}.csv'.format(code_start_time),encoding='utf-8',index=False)
    
    
    return result_pivot

def make_text_message(**kwargs):
    result_pivot=kwargs['task_instance'].xcom_pull(task_ids='data_transform_result')
    date_info=datetime.datetime.now(timezone('Asia/Seoul'))
    
    if date_info.strftime('%H')=='06': ## 시간대가 오전 6시면
        work_or_home='출근'
    
    elif date_info.strftime('%H')=='17': ## 시간대가 오후 5시면 ## 일단 테스트 용으로
        work_or_home='퇴근'
    else: ## 확인용도로 나중에제거할예정
        work_or_home='퇴근'
    
    result_pivot['낙뢰']=result_pivot['낙뢰'].apply(lambda x : int(x))
    result_pivot['강수형태']=result_pivot['강수형태'].apply(lambda x : int(x))
    result_pivot['습도']=result_pivot['습도'].apply(lambda x : int(x))
    result_pivot['1시간강수량']=result_pivot['1시간강수량'].apply(lambda x : 0 if x=='강수없음' else int(x))
    result_pivot['하늘상태']=result_pivot['하늘상태'].apply(lambda x : int(x))
    result_pivot['기온']=result_pivot['기온'].apply(lambda x : int(x))
    result_pivot['동서바람성분']=result_pivot['동서바람성분'].apply(lambda x : float(x))
    result_pivot['남북바람성분']=result_pivot['남북바람성분'].apply(lambda x : float(x))
    result_pivot['풍향']=result_pivot['풍향'].apply(lambda x : float(x))
    result_pivot['풍속']=result_pivot['풍속'].apply(lambda x : float(x))
    def rain_category_summary(input_PTY): ## 강수형태에 대한 멘트
        rain_data=['비 안옴','비 옴','비 또는 눈','정보X','빗방울 내림','빗방울 또는 눈날림','눈날림']
        return rain_data[int(input_PTY)]
    def rain_value_summary(input_RN1): ## 1시간 강수량에 대한 멘트
        if input_RN1==0:
            return "우산 없어도 됨"
        elif 0<input_RN1<=2:
            return "비 오긴하는데, 우산없어도 이정도는 괜찮음"
        elif 2<input_RN1<=6:
            return "우산 챙겨야함, 어느정도 옴"
        elif 6<input_RN1:
            return "우산 큰거 챙겨야함, 비 많이옴"
    def sky_summary(input_SKY): ## 하늘상태에 대한 멘트
        sky_data=['정보X','맑음','정보X','구름많음','흐림']
        return sky_data[input_SKY]
    def wind_power_summary(input_WSD): ## 풍속 세기에 대한 멘트 
        if input_WSD<4:
            return "바람이 약하다"
        elif 4<=input_WSD<9:
            return "바람이 약간 강하다"
        elif 9<=input_WSD<14:
            return "바람이 강하다"
        elif 14<input_WSD:
            return "바람이 매우 강하다"
    result_pivot['강수형태해석']=result_pivot['강수형태'].apply(lambda x : rain_category_summary(x)) ## 강수형태
    result_pivot['강수량해석']=result_pivot['1시간강수량'].apply(lambda x : rain_value_summary(x)) ## 강수량 해석
    result_pivot['하늘해석']=result_pivot['하늘상태'].apply(lambda x : sky_summary(x)) ## 하늘상태 해석
    result_pivot['풍속해석']=result_pivot['풍속'].apply(lambda x : wind_power_summary(x)) ## 풍속 해석
    
    if sum(result_pivot['1시간강수량'])==0:
        rain_result='비안옴'
    else:
        rain_result='비옴'
        
    mean_rain=round(np.mean(result_pivot['1시간강수량'].values),1)
    max_rain=max(result_pivot['1시간강수량'].values)
    mean_wind=round(np.mean(result_pivot['풍속'].values),1)
    make_text_message = f'''  ★결론★
    날씨 {sky_summary(max(result_pivot['하늘상태'].values))}, *{work_or_home} 하는동안 {rain_result}*
    \n
    ★날씨 해석★
    {rain_category_summary(max(result_pivot['강수형태'].values))}
    {rain_value_summary(mean_rain)}
    {wind_power_summary(mean_wind)}
    \n
    ★수치 정보★
    평균강수량 : {mean_rain} mm
    최대강수량 : {max_rain} mm
    평균기온 : {round(np.mean(result_pivot['기온'].values),1)} ℃
    평균풍속 : {mean_wind} m/s'''
    return make_text_message

def send_to_slack(**kwargs):
    make_text_message=kwargs['task_instance'].xcom_pull(task_ids='make_text_message')
    slack_message = {
        'text': {make_text_message}
    }

    return slack_message
