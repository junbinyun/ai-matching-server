// src/server.js

require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 4000;
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:3000";

// -----------------------------
// 기본 미들웨어 설정
// -----------------------------
app.use(cors({
  origin: CLIENT_URL,
  credentials: true,
}));

app.use(express.json());

// -----------------------------
// Socket.io 서버 설정
// -----------------------------
const io = new Server(server, {
  cors: {
    origin: CLIENT_URL,
    methods: ["GET", "POST"],
    credentials: true,
  },
});

// -----------------------------
// OpenAI 클라이언트 설정
// API 키가 없으면 null로 둠
// -----------------------------
let openai = null;

if (process.env.OPENAI_API_KEY) {
  const OpenAI = require("openai");
  const OpenAIClient = OpenAI.default || OpenAI;

  openai = new OpenAIClient({
    apiKey: process.env.OPENAI_API_KEY,
  });

  console.log("OpenAI API 연결 준비 완료");
} else {
  console.log("OpenAI API 키 없음 → 키워드 기반 감정 분석 사용");
}

// -----------------------------
// 대기열 저장 공간
// 실제 서비스에서는 DB나 Redis를 쓰는 게 좋지만,
// 지금은 연습용이라 메모리에 저장
// -----------------------------
const waitingUsers = [];

// 현재 만들어진 방 목록
const rooms = new Map();

// -----------------------------
// 기본 접속 확인 API
// -----------------------------
app.get("/", (req, res) => {
  res.send("AI Matching Socket Server is running");
});

// -----------------------------
// 현재 서버 상태 확인 API
// -----------------------------
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    waitingUserCount: waitingUsers.length,
    roomCount: rooms.size,
  });
});

// -----------------------------
// REST 방식 감정 분석 테스트 API
// 프론트에서 fetch로 테스트 가능
// -----------------------------
app.post("/api/analyze-emotion", async (req, res) => {
  try {
    const { text } = req.body;

    if (!text || text.trim() === "") {
      return res.status(400).json({
        ok: false,
        message: "분석할 문장을 입력해주세요.",
      });
    }

    const result = await analyzeEmotion(text);

    res.json({
      ok: true,
      result,
    });
  } catch (error) {
    console.error("감정 분석 API 오류:", error);

    res.status(500).json({
      ok: false,
      message: "감정 분석 중 서버 오류가 발생했습니다.",
    });
  }
});

// -----------------------------
// Socket.io 연결 처리
// -----------------------------
io.on("connection", (socket) => {
  console.log("사용자 접속:", socket.id);

  // 사용자가 매칭 대기열에 들어올 때
  socket.on("join_queue", async (payload) => {
    try {
      const nickname = payload?.nickname || "익명";
      const text = payload?.text || "";
      const tags = Array.isArray(payload?.tags) ? payload.tags : [];

      if (!text.trim()) {
        socket.emit("queue_error", {
          message: "감정이나 고민 내용을 입력해주세요.",
        });
        return;
      }

      // 1. 사용자의 문장을 감정 분석
      const emotionData = await analyzeEmotion(text);

      const currentUser = {
  socketId: socket.id,
  nickname,
  text,
  tags,
  emotion: emotionData.emotion,
  intensity: emotionData.intensity,
  topic: emotionData.topic,
  confidence: emotionData.confidence ?? 0.7,
  joinedAt: Date.now(),
};

// 태그와 실제 문장 감정이 심하게 다르면 매칭 중단
const validation = validateMatchingRequest(currentUser);

if (!validation.ok) {
  socket.emit("queue_error", {
    message: validation.message,
    analysis: {
      emotion: currentUser.emotion,
      intensity: currentUser.intensity,
      topic: currentUser.topic,
    },
  });

  console.log("매칭 요청 거절:", validation.reason, currentUser);
  return;
}

console.log("매칭 대기 요청:", currentUser);

      // 2. 대기열에서 가장 잘 맞는 사용자 찾기
      const matchedUser = findBestMatch(currentUser);

      // 3. 매칭 상대가 있으면 방 생성
      if (matchedUser) {
        removeWaitingUser(matchedUser.socketId);

        const roomId = createRoomId();

        socket.join(roomId);

        const matchedSocket = io.sockets.sockets.get(matchedUser.socketId);
        if (matchedSocket) {
          matchedSocket.join(roomId);
        }

        const roomInfo = {
          roomId,
          users: [
            {
              socketId: currentUser.socketId,
              nickname: currentUser.nickname,
              emotion: currentUser.emotion,
              topic: currentUser.topic,
            },
            {
              socketId: matchedUser.socketId,
              nickname: matchedUser.nickname,
              emotion: matchedUser.emotion,
              topic: matchedUser.topic,
            },
          ],
          createdAt: Date.now(),
        };

        rooms.set(roomId, roomInfo);

        io.to(roomId).emit("matched", {
          message: "매칭이 완료되었습니다.",
          room: roomInfo,
        });

        console.log("매칭 완료:", roomInfo);
      } else {
        // 4. 매칭 상대가 없으면 대기열에 넣기
        waitingUsers.push(currentUser);

        socket.emit("waiting", {
          message: "비슷한 감정의 사용자를 기다리는 중입니다.",
          user: {
            nickname: currentUser.nickname,
            emotion: currentUser.emotion,
            intensity: currentUser.intensity,
            topic: currentUser.topic,
          },
        });

        console.log("대기열 추가:", waitingUsers.length);
      }
    } catch (error) {
      console.error("join_queue 오류:", error);

      socket.emit("queue_error", {
        message: "매칭 처리 중 오류가 발생했습니다.",
      });
    }
  });

  // 채팅 메시지 전송
  socket.on("send_message", (payload) => {
    const { roomId, message, nickname } = payload || {};

    if (!roomId || !message) {
      socket.emit("chat_error", {
        message: "roomId와 message가 필요합니다.",
      });
      return;
    }

    const chatMessage = {
      roomId,
      nickname: nickname || "익명",
      message,
      sentAt: new Date().toISOString(),
    };

    io.to(roomId).emit("receive_message", chatMessage);
  });

  // 대기열 취소
  socket.on("cancel_queue", () => {
    removeWaitingUser(socket.id);

    socket.emit("queue_cancelled", {
      message: "매칭 대기를 취소했습니다.",
    });

    console.log("대기 취소:", socket.id);
  });

  // 연결 종료
  socket.on("disconnect", () => {
    console.log("사용자 연결 종료:", socket.id);

    // 대기열에서 제거
    removeWaitingUser(socket.id);

    // 방 안에 있던 사용자라면 상대방에게 알림
    for (const [roomId, roomInfo] of rooms.entries()) {
      const isInRoom = roomInfo.users.some((user) => user.socketId === socket.id);

      if (isInRoom) {
        io.to(roomId).emit("partner_disconnected", {
          message: "상대방이 연결을 종료했습니다.",
        });

        rooms.delete(roomId);
      }
    }
  });
});

// -----------------------------
// 감정 분석 함수
// -----------------------------
async function analyzeEmotion(text) {
  // OpenAI API 키가 있으면 AI 분석 사용
  if (openai) {
    try {
      const response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `
너는 사용자의 감정 상태를 분석하는 도우미다.
반드시 JSON만 출력해라.
형식:
{
  "emotion": "기쁨 | 슬픔 | 분노 | 불안 | 외로움 | 스트레스 | 평온 | 기타",
  "intensity": 1부터 10 사이 숫자,
  "topic": "연애 | 가족 | 학교 | 취업 | 친구 | 건강 | 일상 | 기타"
}
            `,
          },
          {
            role: "user",
            content: text,
          },
        ],
        temperature: 0.2,
      });

      const content = response.choices[0].message.content;
      const parsed = JSON.parse(content);

      return {
        emotion: parsed.emotion || "기타",
        intensity: Number(parsed.intensity) || 5,
        topic: parsed.topic || "기타",
      };
    } catch (error) {
      console.error("OpenAI 감정 분석 실패 → 키워드 분석으로 대체:", error.message);
      return keywordEmotionAnalysis(text);
    }
  }

  // API 키가 없으면 키워드 분석 사용
  return keywordEmotionAnalysis(text);
}

// -----------------------------
// 간단한 키워드 기반 감정 분석
// API 없이도 테스트 가능
// -----------------------------
function keywordEmotionAnalysis(text) {
  const lowerText = text.toLowerCase();

  const emotionKeywords = {
    기쁨: ["기쁘", "행복", "좋아", "신나", "설레", "웃", "재밌", "기분 좋"],
    슬픔: ["슬프", "눈물", "우울", "힘들", "괴로", "상처", "무기력"],
    분노: ["화나", "짜증", "열받", "빡", "분노", "억울"],
    불안: ["불안", "걱정", "무서", "두려", "초조", "긴장"],
    외로움: ["외롭", "혼자", "쓸쓸", "고독"],
    스트레스: ["스트레스", "압박", "지침", "피곤", "번아웃"],
    평온: ["괜찮", "편안", "평온", "차분"],
  };

  const topicKeywords = {
    연애: ["연애", "남친", "여친", "헤어", "이별", "썸", "고백", "짝사랑"],
    가족: ["가족", "엄마", "아빠", "부모", "형", "누나", "동생"],
    학교: ["학교", "과제", "시험", "교수", "수업", "학점"],
    취업: ["취업", "면접", "회사", "직장", "알바", "진로"],
    친구: ["친구", "우정", "배신", "관계"],
    건강: ["건강", "아파", "병원", "운동", "잠"],
  };

  const emotionScores = {};

  for (const [emotion, keywords] of Object.entries(emotionKeywords)) {
    emotionScores[emotion] = 0;

    for (const keyword of keywords) {
      if (lowerText.includes(keyword)) {
        emotionScores[emotion] += 1;
      }
    }
  }

  // 상담 서비스에서는 부정 감정을 긍정보다 우선한다.
  // 예: "설레는데 불안하다" → 기쁨보다 불안으로 보는 게 안전함
  const negativePriority = ["불안", "슬픔", "외로움", "스트레스", "분노"];

  for (const emotion of negativePriority) {
    if (emotionScores[emotion] > 0) {
      let detectedTopic = "기타";

      for (const [topic, keywords] of Object.entries(topicKeywords)) {
        if (keywords.some((keyword) => lowerText.includes(keyword))) {
          detectedTopic = topic;
          break;
        }
      }

      return {
        emotion,
        intensity: estimateIntensity(text),
        topic: detectedTopic,
        confidence: 0.8,
      };
    }
  }

  // 부정 감정이 없을 때만 긍정 감정 판단
  let detectedEmotion = "기타";

  if (emotionScores["기쁨"] > 0) {
    detectedEmotion = "기쁨";
  } else if (emotionScores["평온"] > 0) {
    detectedEmotion = "평온";
  }

  let detectedTopic = "기타";

  for (const [topic, keywords] of Object.entries(topicKeywords)) {
    if (keywords.some((keyword) => lowerText.includes(keyword))) {
      detectedTopic = topic;
      break;
    }
  }

  return {
    emotion: detectedEmotion,
    intensity: estimateIntensity(text),
    topic: detectedTopic,
    confidence: detectedEmotion === "기타" ? 0.3 : 0.7,
  };
}

// -----------------------------
// 감정 강도 대충 추정
// -----------------------------
function estimateIntensity(text) {
  let intensity = 5;

  if (text.includes("너무") || text.includes("진짜") || text.includes("완전")) {
    intensity += 2;
  }

  if (text.includes("죽을") || text.includes("미치") || text.includes("최악")) {
    intensity += 2;
  }

  if (text.includes("조금") || text.includes("약간")) {
    intensity -= 2;
  }

  if (intensity < 1) intensity = 1;
  if (intensity > 10) intensity = 10;

  return intensity;
}

// -----------------------------
// 감정 그룹 분류
// -----------------------------
function getEmotionGroup(emotion) {
  const negativeEmotions = ["슬픔", "불안", "외로움", "스트레스"];
  const positiveEmotions = ["기쁨", "평온"];
  const angerEmotions = ["분노"];

  if (negativeEmotions.includes(emotion)) return "negative";
  if (positiveEmotions.includes(emotion)) return "positive";
  if (angerEmotions.includes(emotion)) return "anger";

  return "unknown";
}

// -----------------------------
// 태그에서 감정 성격 추출
// -----------------------------
function getEmotionGroupsFromTags(tags) {
  const groups = new Set();

  for (const tag of tags) {
    if (tag.includes("우울") || tag.includes("불안")) {
      groups.add("negative");
    }

    if (tag.includes("번아웃")) {
      groups.add("negative");
    }
  }

  return Array.from(groups);
}

// -----------------------------
// 감정 그룹끼리 호환 가능한지 확인
// -----------------------------
function areEmotionGroupsCompatible(groupA, groupB) {
  if (groupA === groupB) return true;

  return false;
}

// -----------------------------
// 태그와 실제 문장 감정이 충돌하는지 검사
// -----------------------------
function validateMatchingRequest(user) {
  const textEmotionGroup = getEmotionGroup(user.emotion);
  const tagEmotionGroups = getEmotionGroupsFromTags(user.tags);

  // 상담 서비스에서는 순수 긍정 감정은 매칭 대상에서 제외
  // 예: "너무 기쁘다", "설렌다", "행복하다"
  if (textEmotionGroup === "positive") {
    return {
      ok: false,
      reason: "POSITIVE_EMOTION_NOT_COUNSELING_TARGET",
      message:
        `현재 작성한 내용은 "${user.emotion}" 감정에 가까워 보여요. ` +
        `이 서비스는 고민이나 어려움을 나누는 상담 매칭 서비스라서, ` +
        `고민 내용을 조금 더 구체적으로 작성해주세요.`,
    };
  }

  // 감정 관련 태그가 없으면 검사하지 않음
  if (tagEmotionGroups.length === 0) {
    return { ok: true };
  }

  // 감정 분석이 애매하면 일단 허용
  if (textEmotionGroup === "unknown") {
    return { ok: true };
  }

  const hasCompatibleTag = tagEmotionGroups.some((tagGroup) =>
    areEmotionGroupsCompatible(tagGroup, textEmotionGroup)
  );

  // 예: 태그는 우울/불안인데 실제 내용은 기쁨/평온/분노
  if (!hasCompatibleTag) {
    return {
      ok: false,
      reason: "TAG_TEXT_EMOTION_CONFLICT",
      message:
        `선택한 태그와 작성한 내용의 감정이 달라 보여요. ` +
        `현재 내용은 "${user.emotion}" 감정에 가까워 보여서, ` +
        `고민 태그를 다시 선택하거나 내용을 수정해주세요.`,
    };
  }

  return { ok: true };
}

// -----------------------------
// 두 사용자의 감정이 매칭 가능한지 확인
// -----------------------------
function isEmotionCompatible(userA, userB) {
  const groupA = getEmotionGroup(userA.emotion);
  const groupB = getEmotionGroup(userB.emotion);

  // 감정이 완전히 같으면 매칭 가능
  if (userA.emotion === userB.emotion) {
    return true;
  }

  // 긍정 감정은 긍정 감정끼리만
  if (groupA === "positive" || groupB === "positive") {
    return groupA === groupB;
  }

  // 분노는 일단 분노끼리만
  if (groupA === "anger" || groupB === "anger") {
    return groupA === groupB;
  }

  // 부정 감정끼리는 매칭 가능
  if (groupA === "negative" && groupB === "negative") {
    return true;
  }

  // 감정 분석이 애매하면 태그가 같을 때만 허용
  if (groupA === "unknown" || groupB === "unknown") {
    const commonTags = userA.tags.filter((tag) => userB.tags.includes(tag));
    return commonTags.length > 0;
  }

  return false;
}
// -----------------------------
// 가장 잘 맞는 사용자 찾기
// -----------------------------
// -----------------------------
// 가장 잘 맞는 사용자 찾기
// -----------------------------
function findBestMatch(currentUser) {
  if (waitingUsers.length === 0) {
    return null;
  }

  let bestMatch = null;
  let bestScore = -1;

  for (const waitingUser of waitingUsers) {
    // 감정이 너무 다르면 후보에서 제외
    if (!isEmotionCompatible(currentUser, waitingUser)) {
      console.log("감정 불일치로 매칭 제외:", {
        currentUser: {
          emotion: currentUser.emotion,
          tags: currentUser.tags,
          text: currentUser.text,
        },
        waitingUser: {
          emotion: waitingUser.emotion,
          tags: waitingUser.tags,
          text: waitingUser.text,
        },
      });

      continue;
    }

    const score = calculateMatchScore(currentUser, waitingUser);

    if (score > bestScore) {
      bestScore = score;
      bestMatch = waitingUser;
    }
  }

  // 기준 점수를 기존보다 높임
  if (bestScore < 60) {
    return null;
  }

  return bestMatch;
}

// -----------------------------
// 매칭 점수 계산
// -----------------------------
function calculateMatchScore(userA, userB) {
  let score = 0;

  // 감정이 완전히 같으면 큰 점수
  if (userA.emotion === userB.emotion) {
    score += 70;
  }
  // 같은 감정 그룹이면 중간 점수
  else if (getEmotionGroup(userA.emotion) === getEmotionGroup(userB.emotion)) {
    score += 45;
  }

  // 주제가 같으면 점수 추가
  if (userA.topic === userB.topic) {
    score += 20;
  }

  // 감정 강도가 비슷하면 점수 추가
  const intensityDiff = Math.abs(userA.intensity - userB.intensity);

  if (intensityDiff <= 2) {
    score += 10;
  } else if (intensityDiff <= 4) {
    score += 5;
  }

  // 태그는 보조 기준으로만 사용
  const commonTags = userA.tags.filter((tag) => userB.tags.includes(tag));
  score += Math.min(commonTags.length * 5, 10);

  return score;
}

// -----------------------------
// 대기열에서 사용자 제거
// -----------------------------
function removeWaitingUser(socketId) {
  const index = waitingUsers.findIndex((user) => user.socketId === socketId);

  if (index !== -1) {
    waitingUsers.splice(index, 1);
  }
}

// -----------------------------
// 방 ID 생성
// -----------------------------
function createRoomId() {
  return `room_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
}

// -----------------------------
// 서버 실행
// -----------------------------
server.listen(PORT, () => {
  console.log(`서버 실행 중: http://localhost:${PORT}`);
});
