# 🌐 G-Valuemap 배포 가이드

친구들에게 공유할 수 있는 온라인 버전을 만드는 방법입니다.

## 1단계: GitHub 저장소 만들기
1. [GitHub](https://github.com)에 로그인 (없으면 가입하세요)
2. **New Repository** 클릭
   - Repository name: `g-valuemap` (자유롭게)
   - Public 선택
   - **Create repository** 클릭
3. 생성된 저장소 주소 복사 (예: `https://github.com/사용자명/g-valuemap.git`)

## 2단계: 코드 업로드
PC 터미널(PowerShell)에서 다음 명령어를 순서대로 실행하세요:

```powershell
# 1. 원격 저장소 연결 (주소는 본인 것으로 변경 필수!)
git remote add origin https://github.com/사용자명/g-valuemap.git

# 2. 코드 업로드
git push -u origin master
```

## 3단계: Streamlit Cloud 배포
1. [Streamlit Cloud](https://share.streamlit.io) 접속 및 로그인
2. **New app** 클릭
3. **Use existing repo** 선택
4. `g-valuemap` 저장소 선택
   - Branch: `master`
   - Main file path: `app.py`
5. **Deploy!** 클릭

---
이제 `https://g-valuemap.streamlit.app` 같은 주소로 친구들과 접속할 수 있습니다! 🚀 
서버는 Streamlit에서 무료로 제공하며, 항상 켜져 있습니다.
