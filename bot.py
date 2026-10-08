#Start of bot code

#NewsSystemTemplate
#NewsSystemTemplate

import time
import json
import requests
import base64
from datetime import datetime
import calendar
import uuid
import threading
from threading import Thread
import queue as Queue
import sys
from sys import platform
import os

Sessionid = ''
username = ''
MobileNumber = ''
forumID = '86dbeafd-0acb-463c-8079-78da13974e74'

GATEWAY = os.environ.get('GATEWAY_URL', 'http://localhost:8080')
DB_PATH = os.environ.get('DB_PATH', 'news.db')

# ── Smart Notification Keywords ──────────────────────────────────
NOTIFICATION_KEYWORDS = {
    'AI': ['ai', 'llm', 'gpt', 'openai', 'claude', 'gemini', 'deepmind', 'machine learning', 'model', 'anthropic'],
    'Cybersecurity': ['security', 'vulnerability', 'cve', 'hack', 'breach', 'exploit', 'quantum', 'auth', 'cipher'],
    'Bitcoin': ['bitcoin', 'btc', 'crypto', 'blockchain', 'ethereum'],
    'OpenAI': ['openai', 'altman', 'chatgpt', 'o1', 'o3', 'sora']
}
# ──────────────────────────────────────────────────────────────────

def reportToGateway(flow, reply, mobile=''):
    """Report bot events to local gateway so they appear on http://localhost:8080"""
    try:
        payload = {'flow': flow, 'reply': str(reply), 'mobile': mobile}
        requests.post(GATEWAY + '/api/bot-event', json=payload, timeout=5)
    except Exception as e:
        print('[gateway-report] error:', e)

def saveHitsToDb(hits):
    """Save fetched news hits to SQLite database so the local website is updated."""
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS headlines (
            id INTEGER PRIMARY KEY, title TEXT, url TEXT, time INTEGER, score INTEGER, author TEXT)""")
        for hit in hits:
            if not hit.get("title") or not hit.get("objectID"):
                continue
            row = (int(hit["objectID"]), hit.get("title"), hit.get("url"),
                   hit.get("created_at_i"), hit.get("points"), hit.get("author"))
            conn.execute("INSERT OR IGNORE INTO headlines VALUES (?,?,?,?,?,?)", row)
        conn.commit()
        conn.close()
    except Exception as e:
        print('[saveHitsToDb error]:', e)

def sendBridge(bridgeData, replyFlow, replyData):
    """Send formatted response or live news update back to Cosmitude Bridge cloud platform."""
    try:
        global Sessionid
        global forumID
        
        TemplateID = bridgeData.get("TemplateID") if (bridgeData and isinstance(bridgeData, dict)) else "NewsSystemTemplate"
        ForumID = bridgeData.get('ForumID', forumID) if (bridgeData and isinstance(bridgeData, dict)) else forumID
        
        current_dir_path = os.getcwd()
        filePath = open(os.path.join(current_dir_path, 'NewsSystemTemplate_NewsSystemTemplate_Admin.txt'), 'r')
        content = filePath.read()
        filePath.close()
        jsonData = json.loads(content)
        
        adminFlows = jsonData.get('AdminFlow', {})
        if replyFlow in adminFlows:
            FID = adminFlows[replyFlow]["FID"]
        else:
            firstKey = list(adminFlows.keys())[0] if adminFlows else replyFlow
            FID = adminFlows.get(firstKey, {}).get("FID", "")

        tempJson ={
        "ForumID": ForumID,
        "SessionID": Sessionid,
        "MACAddress": 'Bridge-Web',
        "Time": str(int(time.time() * 1000)),
        "ScheduledDateTime": "now",
        "ScheduledBoolean": 0,
        "FlowID": replyFlow,
        "EnableChat": 1,
        "FlowType": "Custom",
        "BridgeForward": 0,
        "TemplateID": TemplateID,
        "TextCount": 1,
        "ImageCount": 1,
        "InvoiceID": "",
        "DocumentCount": 0,
        "User": False,
        "VideoCount": 0,
        "ReplyBridgeID": "",
        "HiddenFlow": False,
        "TempBridgeId": str(uuid.uuid4()),
        replyFlow: replyData,
        "FID": FID,
        "ServerID":'c9b6722d-5dbf-4b4f-a28e-692b4d26c1cf-7870eb8e-2f45-458a-9a70-d6b2d71ee871',
        "SentTo":'0'
        }
        variab = {"Data":json.dumps(tempJson)}
        up = {}
        response = requests.post('https://ca.cosmitude.com/' + 'bridgeSendingToForum', files=up, data=variab, timeout=600)
        print(f"[sendBridge] Sent to {replyFlow}: {response.status_code}")
    except Exception as e:
        print("[sendBridge error]:", e)

def listen_for_live_news():
    """Background listener: evaluates smart notification keyword filters and pushes targeted alerts."""
    time.sleep(3)
    print('[live-listener] Started listening for new news to auto-push & evaluate smart notification filters...')
    while True:
        try:
            r = requests.get(GATEWAY + '/api/headlines/stream', stream=True, timeout=(5, 60))
            for line in r.iter_lines():
                if not line:
                    continue
                line_str = line.decode('utf-8', errors='ignore')
                if line_str.startswith('data: '):
                    raw_json = line_str[6:].strip()
                    try:
                        item = json.loads(raw_json)
                        title = item.get('title', 'No Title')
                        url = item.get('url') or 'no link'
                        t_lower = title.lower()

                        matched_topics = []
                        for category, keywords in NOTIFICATION_KEYWORDS.items():
                            if any(k in t_lower for k in keywords):
                                matched_topics.append(category)

                        if matched_topics:
                            topic_str = ", ".join(matched_topics)
                            msg = f"🔔 SMART NOTIFICATION [{topic_str}]\nHeadline: {title}\nLink: {url}"
                            print(f"[smart-notification] Matched {topic_str}: {title}")
                            reportToGateway(f"SmartNotify:{topic_str}", msg)
                            sendBridge(None, 'RealTimeUpdates', msg)
                        else:
                            msg = f"📰 NEW HEADLINE FETCHED:\n- {title}\nLink: {url}"
                            print(f"[live-listener] New story detected: {title}")
                            reportToGateway('AutoNewsPush', msg)
                            sendBridge(None, 'RealTimeUpdates', msg)
                    except Exception as parse_err:
                        print('[live-listener] Error parsing SSE json:', parse_err)
        except Exception:
            time.sleep(5)

# ---- Flow handlers ----

def handle_NewsAPIs():
    try:
        url = 'https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=30'
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            hits = r.json().get('hits', [])
            if hits:
                saveHitsToDb(hits)
                top = hits[0].get('title', '')
                return f"Fetched {len(hits)} top stories. #1: {top}"
        return "Fetched news stories."
    except Exception as e:
        return 'News service error: ' + str(e)

def handle_BackendSetup():
    return 'Backend setup: news_backend.py (port 8000), gateway.py (port 8080).'

def handle_DatabaseSelection():
    return 'Database: SQLite (news.db).'

def handle_DatabaseSchema():
    return 'Schema: headlines (id PRIMARY KEY, title, url, time, score, author).'

def handle_RealTimeUpdates():
    return 'Real-Time Updates active via SSE stream.'

FLOW_HANDLERS = {
    'NewsAPIs':          handle_NewsAPIs,
    'BackendSetup':      handle_BackendSetup,
    'DatabaseSelection': handle_DatabaseSelection,
    'DatabaseSchema':    handle_DatabaseSchema,
    'RealTimeUpdates':   handle_RealTimeUpdates,
}

def go():
    try:
        login()
        threading.Thread(target=listen_for_live_news, daemon=True).start()
        
        while True:
            clienntSync()
            time.sleep(10)
    except Exception as e:
        print (e)
        return e

def login():
    try:
        global Sessionid
        global username
        global MobileNumber
        global forumID
        tempJson = {
        "MACAddress": 'Bridge-Web',
        "UserName":'@uuidb41322f3b6a34df1a69dc46d18d39569',
        "Password":base64.b64decode('Q29zbWl0dWRlQnJpZGdlRGV2aWNl').decode('UTF-8'),
        "ServerID":'c9b6722d-5dbf-4b4f-a28e-692b4d26c1cf-7870eb8e-2f45-458a-9a70-d6b2d71ee871'
        }
        headers = {'Content-Type': 'application/json'}
        variab =json.dumps(tempJson)
        response = requests.post('https://ca.cosmitude.com/' + 'loginForDevice', data=variab,headers=headers, timeout=600)
        strData = str(response.text)
        respJson = json.loads(strData)
        Sessionid = respJson['ErrorMessage']['SessionID']
        countryCode = MobileNumber = respJson['ErrorMessage']['CountryCode']
        MobileNumber = countryCode+respJson['ErrorMessage']['MobileNumber']
        print(f"[Cosmitude Login] Success! SessionID: {Sessionid}")
        getForumDetails()
    except Exception as e:
        print (e)
        return e

def getForumDetails():
    try:
        tempJson = {
        "MACAddress": 'Bridge-Web',
        "ForumID":forumID,
        "SessionID":Sessionid,
        "ServerID":'c9b6722d-5dbf-4b4f-a28e-692b4d26c1cf-7870eb8e-2f45-458a-9a70-d6b2d71ee871'
        }
        headers = {'Content-Type': 'application/json'}
        variab =json.dumps(tempJson)
        response = requests.post('https://ca.cosmitude.com/' + 'getRequestedForumDetails', data=variab,headers=headers, timeout=600)
        if response.status_code == 200:
            responseStr = str(response.text)
            respJson = json.loads(responseStr)
            errorCode = respJson['ErrorCode']
            if errorCode == 1172:
                errorMsg = respJson['ErrorMessage']
                forumDetails = errorMsg['ForumDataArray']
                for singleForum in forumDetails:
                    singleForumDetails = singleForum[forumID]
                    saveForumDetails = singleForumDetails['NewForumJsonData']
                    with open('NewsSystemTemplate_NewsSystemTemplate_Admin.txt', 'w') as f:
                        json.dump(saveForumDetails, f)
                    print("[Cosmitude] Updated NewsSystemTemplate_NewsSystemTemplate_Admin.txt")
    except Exception as e:
        print (e)
        return e

def get_handler_for_flow(flow_name):
    if flow_name in FLOW_HANDLERS:
        return FLOW_HANDLERS[flow_name]
    clean_name = flow_name.split('_')[-1] if '_' in flow_name else flow_name
    if clean_name in FLOW_HANDLERS:
        return FLOW_HANDLERS[clean_name]
    return None

def clienntSync():
    try:
        clientSyncJson = {
        "SessionID":Sessionid,
        "MACAddress":'Bridge-Web',
        "ServerID":'c9b6722d-5dbf-4b4f-a28e-692b4d26c1cf-7870eb8e-2f45-458a-9a70-d6b2d71ee871',
        "ServerName":'Bridge'
        }
        headers = {'Content-Type': 'application/json'}
        variab =json.dumps(clientSyncJson)
        response = requests.post('https://ca.cosmitude.com/' + 'syncUserData', data=variab,headers=headers, timeout=600)
        strData = str(response.text)
        dataRead = json.loads(strData)
        
        syncData = dataRead.get('ErrorMessage', {}).get('Admin', []) if isinstance(dataRead.get('ErrorMessage'), dict) else []
        if syncData:
            for data in syncData:
                if 'Bridge' in data:
                    bridgeData = data['Bridge']
                    flowName = bridgeData.get('FlowID', '')
                    mNumber  = bridgeData.get('MobileNumber', '')
                    print(f"[sync] Received FlowID={flowName!r} MobileNumber={mNumber!r}")

                    if ("NewsAPIs" in flowName or "NewsSystemTemplate_NewsAPIs" in flowName) and MobileNumber != mNumber:
                        reportToGateway(flowName, "Fetching Open API news data...", mNumber)
                        que1 = Queue.Queue()
                        t1 = Thread(target=lambda q, arg1: q.put(openApi1(arg1)), args=(que1, bridgeData))
                        t1.start()
                        t1.join()
                    else:
                        handler = get_handler_for_flow(flowName)
                        replyText = handler() if handler else f"Processed operation for {flowName}"
                        reportToGateway(flowName, replyText, mNumber)
                        que = Queue.Queue()
                        t = Thread(target=lambda q, arg1, arg2, arg3: q.put(sendBridge(arg1, arg2, arg3)), args=(que, bridgeData, flowName, replyText))
                        t.start()

                elif 'UpdatedTemplateJson' in data:
                    templateForAdmin = data['UpdatedTemplateJson']
                    forumIDCheck = templateForAdmin.get('ForumID')
                    if forumIDCheck == forumID:
                        with open('NewsSystemTemplate_NewsSystemTemplate_Admin.txt', 'w') as f:
                            json.dump(templateForAdmin, f)
    except Exception as e:
        print ("[clienntSync error]:", e)

def openApi1(bridgeData):
    try:
        url = 'https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=30'
        response = requests.get(url, timeout=15)
        responseData = response.text if response.status_code == 200 else json.loads(response.text)
        json_object = json.loads(responseData)
        hits = json_object.get('hits', [])
        
        saveHitsToDb(hits)
        
        if hits:
            top_title = hits[0].get('title', '')
            reportToGateway('NewsAPIs', f"Fetched {len(hits)} hits. Top: {top_title}")
        
        sendData = {
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_2' : base64.b64encode(str(hits).encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_4' : base64.b64encode(str(hits).encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_6' : base64.b64encode(str(hits).encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_8' : base64.b64encode(str(hits).encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_10' : base64.b64encode(''.encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_12' : base64.b64encode(''.encode("utf-8")).decode("utf-8"),
            'NewsSystemTemplate_DatabaseSchema_Main-dc03e037-1831-4e97-9ef3-428424bf6b70_16' : base64.b64encode(''.encode("utf-8")).decode("utf-8")}
            
        que = Queue.Queue()
        t = Thread(target=lambda q, arg1,arg2,arg3: q.put(sendBridge(arg1,arg2,arg3)), args=(que,bridgeData,'NewsSystemTemplate_DatabaseSchema',sendData))
        t.start()
        print(f"[openApi1] Processed {len(hits)} hits and queued sendBridge")
    except Exception as e:
        print("[openApi1 error]:", e)

go()

#End of bot code
