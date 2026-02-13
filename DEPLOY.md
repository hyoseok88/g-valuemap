# 🌐 G-Valuemap 배포 가이드

친구들에게 공유할 수 있는 온라인 버전을 만드는 방법입니다.

## 1단계: GitHub 저장소 만들기
1. [GitHub](https://github.com)에 로그인 (없으면 가입하세요)
2. **New Repository** 클릭
   - Repository name: `g-valuemap` (자유롭게)
   - Public 선택
   - **Create repository** 클릭
3. 생성된 저장소 주소 복사 (예: `https://github.com/사용자명/g-valuemap.git`)

## 2단계: 코드 업로드 (완료됨! ✅)
**이미 제가 업로드했습니다. 이 단계는 건너뛰세요!**
(만약 나중에 직접 수정하고 싶다면 아래 명령어를 참고하세요)

```powershell
# 1. 원격 저장소 연결
git remote add origin https://github.com/hyoseok88/g-valuemap.git

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

## 4단계: 업데이트 방법 (기능 추가 시)
로컬에서 코드를 수정한 후, 다음 명령어를 실행하면 Streamlit Cloud가 자동으로 업데이트됩니다.

```powershell
git add .
git commit -m "Update recent changes"
git push origin master
```
(약 2~3분 뒤에 웹사이트에 반영됩니다)

## ⚠️ 문제 해결: 'git' 명령어를 찾을 수 없나요?
만약 터미널에서 `git` 명령어가 실행되지 않는다면:
1. **Git이 설치되었는지 확인하세요.**
   - [Git 다운로드 링크](https://git-scm.com/downloads)에서 설치해주세요.
   - 설치 시 "Add Git to PATH" 옵션이 체크되어 있는지 확인하세요.
2. **설치 후 터미널을 재시작하세요.**
   - VS Code나 터미널 창을 완전히 껐다가 다시 켜야 PATH가 적용됩니다.
3. **GitHub Desktop 사용하기 (대안)**
   - 명령어가 어렵다면 [GitHub Desktop](https://desktop.github.com/)을 설치하여 마우스 클릭으로 업로드할 수도 있습니다.

---
이제 `https://g-valuemap.streamlit.app` 같은 주소로 친구들과 접속할 수 있습니다! 🚀 
서버는 Streamlit에서 무료로 제공하며, 항상 켜져 있습니다.
