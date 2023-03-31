from fastapi import FastAPI
from aliyun.log import LogClient
# 定义阿里云SDK访问地址、key
endpoint = 'cn-xxxx.log.aliyuncs.com'
accessKeyId = 'xxxxxxxxxxxxxxxx'
accessKey = 'xxxxxxxxxxxxxxx'
client = LogClient(endpoint, accessKeyId, accessKey)

app = FastAPI()
# 统计总流量PV和Flow
def total (begintime,endtime):
    pv = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(total_pv) as pv")
    flow = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(total_flow) as flow")
    total_pv = int(pv.body[0]['pv'])
    total_flow = int(flow.body[0]['flow'])
    return total_pv,total_flow
# 统计image PV和Flow
def image (begintime,endtime):
    pv = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(image_pv) as pv")
    flow = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(image_flow) as flow")
    image_pv = int(pv.body[0]['pv'])
    image_flow = int(flow.body[0]['flow'])
    return image_pv,image_flow
# 统计video PV和Flow
def video (begintime,endtime):
    pv = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(video_pv) as pv")
    flow = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(video_flow) as flow")
    video_pv = int(pv.body[0]['pv'])
    video_flow = int(flow.body[0]['flow'])
    return video_pv,video_flow
# 统计audio PV和Flow
def audio (begintime,endtime):
    pv = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(audio_pv) as pv")
    flow = client.get_log("kaishucdnlogintime", "cdnscheduled", begintime,endtime,query="*|select sum(audio_flow) as flow")
    audio_pv = int(pv.body[0]['pv'])
    audio_flow = int(flow.body[0]['flow'])
    return audio_pv,audio_flow

@app.get('/aliyun/beginDate={fromtime}&endDate={totime}')
def calculate(fromtime: str=None, totime: str=None):
    # 调整时间格式
    fromtime = list(fromtime)
    totime = list(totime)
    f_time = fromtime[0] + fromtime[1] + fromtime[2] + fromtime[3] + "-" + fromtime[4] + fromtime[5] + "-" + fromtime[6] + fromtime[7] + " 0:0:0"
    t_time = totime[0] + totime[1] + totime[2] + totime[3] + "-" + totime[4] + totime[5] + "-" + totime[6] + totime[7] + " 23:59:59"
    # 获取 audio、image、video及totalpv数据
    total_pv = total(f_time,t_time)[0]
    audio_pv = audio(f_time,t_time)[0]
    image_pv = image(f_time,t_time)[0]
    video_pv = video(f_time,t_time)[0]
    other_pv = total_pv - audio_pv - video_pv
    # 获取 audio、image、video及tatal流量数据
    total_flow = round(total(f_time,t_time)[1]/1024/1024/1024,3)
    audio_flow = round(audio(f_time,t_time)[1]/1024/1024/1024,3)
    image_flow = round(image(f_time,t_time)[1]/1024/1024/1024,3)
    video_flow = round(video(f_time,t_time)[1]/1024/1024/1024,3)
    other_flow = total_flow - audio_flow - video_flow - image_flow
    # 计算 audio、image、video百分比
    audio_flow_ratio = round(audio_flow/total_flow*100,3)
    image_flow_ratio = round(image_flow/total_flow*100,3)
    video_flow_ratio = round(video_flow/total_flow*100,3)
    other_flow_ratio = round(other_flow/total_flow*100,3)
    res = {"data":{"total":[{"totalFlow":image_flow,"flowRatio":image_flow_ratio,"pv":image_pv,"type":"image"},{"totalFlow":audio_flow,"flowRatio":audio_flow_ratio,"pv":audio_pv,"type":"audio"},{"totalFlow":video_flow,"flowRatio":video_flow_ratio,"pv":video_pv,"type":"video"},{"totalFlow":other_flow,"flowRatio":other_flow_ratio,"pv":other_pv,"type":"other"}]}}
    return res

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="0.0.0.0",
                port=8080,
                workers=1)
