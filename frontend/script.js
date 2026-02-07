const audio = document.getElementById("audioSource");
const btnPlayPause = document.getElementById("btnPlayPause");
const seekBar = document.getElementById("seekBar");
const currentTimeDisplay = document.getElementById("currentTime");
const durationDisplay = document.getElementById("duration");
const lyricsDisplay = document.getElementById("lyricsDisplay");

const audioSrc = "https://musicstreamingprod.s3.ap-south-1.amazonaws.com/the-weeknd-sao-paulo-feat-anitta-official-audio/master.m3u8";

let lyrics = [];
let currentLyricIndex = -1;
let lyricsLoaded = false;

// Initialize HLS
if (Hls.isSupported()) {
  const hls = new Hls({
    autoStartLoad: true,
    startLevel: -1,
    maxBufferLength: 30,
    maxMaxBufferLength: 60,
    enableWebVTT: true,
    enableCEA708Captions: false,
    debug: false, // Set to true for debugging
  });

  hls.loadSource(audioSrc);
  hls.attachMedia(audio);

  hls.on(Hls.Events.MANIFEST_PARSED, (event, data) => {
    console.log("✅ Manifest parsed");
    console.log("📋 Subtitle tracks:", hls.subtitleTracks);
    
    if (hls.subtitleTracks.length > 0) {
      hls.subtitleTrack = 0;
      console.log("✅ Enabled subtitle track:", hls.subtitleTracks[0]);
    } else {
      console.warn("⚠️ No subtitle tracks in manifest");
    }
  });

  hls.on(Hls.Events.SUBTITLE_TRACK_LOADED, (event, data) => {
    console.log("✅ Subtitle track loaded");
  });

  hls.on(Hls.Events.ERROR, (event, data) => {
    if (data.fatal) {
      console.error("❌ Fatal:", data.type, data.details);
      switch (data.type) {
        case Hls.ErrorTypes.NETWORK_ERROR:
          setTimeout(() => hls.startLoad(), 1000);
          break;
        case Hls.ErrorTypes.MEDIA_ERROR:
          hls.recoverMediaError();
          break;
      }
    } else {
      // Filter out non-critical warnings
      if (!data.details.includes("SUBTITLE") && !data.details.includes("subtitle")) {
        console.warn("⚠️", data.details);
      }
    }
  });

} else if (audio.canPlayType('application/vnd.apple.mpegurl')) {
  audio.src = audioSrc;
  console.log("✅ Safari native HLS");
}

// Setup text tracks when metadata loads
audio.addEventListener('loadedmetadata', () => {
  console.log("✅ Media metadata loaded");
  setTimeout(setupTextTracks, 500); // Small delay to ensure tracks are ready
});

// Also try when data is loaded
audio.addEventListener('loadeddata', () => {
  if (!lyricsLoaded) {
    setTimeout(setupTextTracks, 500);
  }
});

function setupTextTracks() {
  const textTracks = audio.textTracks;
  console.log(`📝 Found ${textTracks.length} text track(s)`);

  if (textTracks.length === 0) {
    console.warn("⚠️ No text tracks available");
    lyricsDisplay.innerHTML = '<div class="lyrics-line">No lyrics available</div>';
    return;
  }

  for (let i = 0; i < textTracks.length; i++) {
    const track = textTracks[i];
    console.log(`Track ${i}: kind=${track.kind}, label=${track.label}, language=${track.language}`);
    
    // Enable the track in hidden mode (we handle display ourselves)
    track.mode = 'hidden';
    
    // Try to load cues immediately if available
    if (track.cues && track.cues.length > 0) {
      console.log(`✅ Track ${i} has ${track.cues.length} cues ready`);
      loadCuesFromTrack(track);
      lyricsLoaded = true;
    } else {
      // Listen for cues to be added
      console.log(`⏳ Waiting for cues on track ${i}...`);
      
      track.addEventListener('load', () => {
        console.log(`✅ Track ${i} load event - ${track.cues?.length || 0} cues`);
        if (track.cues && track.cues.length > 0) {
          loadCuesFromTrack(track);
          lyricsLoaded = true;
        }
      });

      // Also watch for cue changes
      const checkCues = setInterval(() => {
        if (track.cues && track.cues.length > 0 && !lyricsLoaded) {
          console.log(`✅ Cues detected - ${track.cues.length} total`);
          clearInterval(checkCues);
          loadCuesFromTrack(track);
          lyricsLoaded = true;
        }
      }, 500);

      // Stop checking after 10 seconds
      setTimeout(() => clearInterval(checkCues), 10000);
    }
  }
}

function loadCuesFromTrack(track) {
  if (!track.cues || track.cues.length === 0) {
    console.warn("Track has no cues to load");
    return;
  }

  console.log(`📥 Loading ${track.cues.length} cues...`);
  lyrics = []; // Clear existing lyrics
  
  for (let i = 0; i < track.cues.length; i++) {
    const cue = track.cues[i];
    processCue(cue);
  }
  
  console.log(`✅ Processed ${lyrics.length} lyric lines`);
  renderLyrics();
}

function processCue(cue) {
  const currentCue = {
    start: cue.startTime,
    end: cue.endTime,
    words: []
  };

  const text = cue.text || '';
  
  // Parse word-level timestamps: <00:00:10.100>word
  const wordRegex = /<(\d{2}:\d{2}:\d{2}\.\d{3})>([^<]+)/g;
  let match;
  let foundWords = false;

  while ((match = wordRegex.exec(text)) !== null) {
    foundWords = true;
    const wordText = match[2].trim();
    if (wordText) {
      currentCue.words.push({
        time: timeToSeconds(match[1]),
        text: wordText
      });
    }
  }

  if (!foundWords && text.trim()) {
    // No word-level timing, split by spaces
    text.split(/\s+/).filter(w => w.trim()).forEach(word => {
      currentCue.words.push({ 
        time: currentCue.start, 
        text: word.trim() 
      });
    });
  }

  if (currentCue.words.length > 0) {
    lyrics.push(currentCue);
  }
}

function timeToSeconds(timeStr) {
  const parts = timeStr.split(':');
  let seconds = 0;
  if (parts.length === 3) {
    seconds += parseInt(parts[0], 10) * 3600;
    seconds += parseInt(parts[1], 10) * 60;
    seconds += parseFloat(parts[2]);
  } else if (parts.length === 2) {
    seconds += parseInt(parts[0], 10) * 60;
    seconds += parseFloat(parts[1]);
  }
  return seconds;
}

function renderLyrics() {
  lyricsDisplay.innerHTML = '';

  if (lyrics.length === 0) {
    lyricsDisplay.innerHTML = '<div class="lyrics-line">No lyrics found</div>';
    return;
  }

  console.log(`🎨 Rendering ${lyrics.length} lyric lines`);

  lyrics.forEach((cue, index) => {
    const lineDiv = document.createElement('div');
    lineDiv.className = 'lyrics-line';
    lineDiv.id = `cue-${index}`;

    cue.words.forEach((word, wIndex) => {
      const wordSpan = document.createElement('span');
      wordSpan.className = 'word';
      wordSpan.textContent = word.text;
      lineDiv.appendChild(wordSpan);

      if (wIndex < cue.words.length - 1) {
        lineDiv.appendChild(document.createTextNode(' '));
      }
    });

    lineDiv.onclick = () => {
      audio.currentTime = cue.start;
      if (audio.paused) audio.play();
    };

    lyricsDisplay.appendChild(lineDiv);
  });

  console.log("✅ Lyrics rendered");
}

function syncLyrics(curTime) {
  if (lyrics.length === 0) return;

  const activeIndex = lyrics.findIndex(c => curTime >= c.start && curTime < c.end);

  if (activeIndex !== -1 && activeIndex !== currentLyricIndex) {
    // Remove previous active state
    if (currentLyricIndex !== -1) {
      const prev = document.getElementById(`cue-${currentLyricIndex}`);
      if (prev) {
        prev.classList.remove('active');
        prev.classList.add('played');
        prev.querySelectorAll('.word').forEach(w => w.classList.remove('active'));
      }
    }

    // Set new active state
    const active = document.getElementById(`cue-${activeIndex}`);
    if (active) {
      active.classList.add('active');
      active.classList.remove('played');
      currentLyricIndex = activeIndex;
      active.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  }

  // Word-by-word highlighting
  if (activeIndex !== -1) {
    const cue = lyrics[activeIndex];
    const active = document.getElementById(`cue-${activeIndex}`);

    if (active) {
      const words = active.querySelectorAll('.word');
      cue.words.forEach((wc, i) => {
        if (i < words.length) {
          words[i].classList.toggle('active', curTime >= wc.time);
        }
      });
    }
  }
}

// Playback controls
btnPlayPause.addEventListener('click', () => {
  if (audio.paused) {
    audio.play().then(() => {
      btnPlayPause.textContent = '⏸️';
    }).catch(err => {
      console.error("Play error:", err);
    });
  } else {
    audio.pause();
    btnPlayPause.textContent = '▶️';
  }
});

audio.addEventListener('playing', () => {
  btnPlayPause.textContent = '⏸️';
});

audio.addEventListener('pause', () => {
  btnPlayPause.textContent = '▶️';
});

audio.addEventListener('timeupdate', () => {
  const curTime = audio.currentTime;
  const dur = audio.duration;

  if (dur > 0) {
    seekBar.value = (curTime / dur) * 100;
    currentTimeDisplay.textContent = formatTime(curTime);
    durationDisplay.textContent = formatTime(dur);
  }

  syncLyrics(curTime);
});

seekBar.addEventListener('input', () => {
  audio.currentTime = (seekBar.value / 100) * audio.duration;
});

function formatTime(seconds) {
  if (isNaN(seconds)) return '0:00';
  const min = Math.floor(seconds / 60);
  const sec = Math.floor(seconds % 60);
  return `${min}:${sec.toString().padStart(2, '0')}`;
}

console.log("🎵 Music player initialized");