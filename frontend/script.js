const audio = document.getElementById("audioSource");
const btnPlayPause = document.getElementById("btnPlayPause");
const seekBar = document.getElementById("seekBar");
const currentTimeDisplay = document.getElementById("currentTime");
const durationDisplay = document.getElementById("duration");
const lyricsDisplay = document.getElementById("lyricsDisplay");
const qualitySelector = document.getElementById("qualitySelector");

const audioSrc = "https://musicstreamingprod.s3.ap-south-1.amazonaws.com/eminem-without-me-official-music-video/master.m3u8";
// const audioSrc = "https://musicstreamingprod.s3.ap-south-1.amazonaws.com/the-weeknd-sao-paulo-feat-anitta-official-audio/master.m3u8"
// const audioSrc = "https://musicstreamingprod.s3.ap-south-1.amazonaws.com/aayega-maza-ab-barsaat-ka_-andaaz-_-akshay-kumar-_-priyanka-chopra-_-lara-dutta-_-romantic-hindi_-hd/master.m3u8";
// const audioSrc = "https://musicstreamingprod.s3.ap-south-1.amazonaws.com/songs-user_39qysgawaslkggjlmyorhfrego5-1770659582624-haseen_-_talwiinder_nds_rippy_official_visualizer_256kbps/master.m3u8";

let allCues = [];
let displayedCues = new Set();
let currentLyricIndex = -1;
let previousLyricIndex = -1;
let hls = null;

// Initialize HLS with ABR support
if (Hls.isSupported()) {
  hls = new Hls({
    autoStartLoad: true,
    startLevel: -1, // -1 = Auto quality (ABR)
    maxBufferLength: 30,
    maxMaxBufferLength: 60,
    enableWebVTT: true,
    enableCEA708Captions: false,
    debug: false,
    
    // ABR Configuration
    abrEwmaDefaultEstimate: 500000,
    abrBandWidthFactor: 0.95,
    abrBandWidthUpFactor: 0.7,
    abrMaxWithRealBitrate: false,
  });

  hls.loadSource(audioSrc);
  hls.attachMedia(audio);

  hls.on(Hls.Events.MANIFEST_PARSED, (event, data) => {
    console.log("✅ Manifest parsed");
    console.log(`📊 Available quality levels: ${data.levels.length}`);
    
    // Populate quality selector
    populateQualitySelector(data.levels);
    
    if (hls.subtitleTracks.length > 0) {
      hls.subtitleTrack = 0;
      console.log("✅ Subtitle track enabled");
    }
  });

  // Monitor quality level changes
  hls.on(Hls.Events.LEVEL_SWITCHING, (event, data) => {
    console.log(`🔄 Switching to level ${data.level}`);
  });

  hls.on(Hls.Events.LEVEL_SWITCHED, (event, data) => {
    const level = hls.levels[data.level];
    const bitrate = Math.round(level.bitrate / 1000);
    console.log(`✅ Now playing: ${bitrate}kbps`);
    updateQualityDisplay(data.level);
  });

  hls.on(Hls.Events.ERROR, (event, data) => {
    if (data.fatal) {
      switch (data.type) {
        case Hls.ErrorTypes.NETWORK_ERROR:
          console.error("❌ Network error, retrying...");
          setTimeout(() => hls.startLoad(), 1000);
          break;
        case Hls.ErrorTypes.MEDIA_ERROR:
          console.error("❌ Media error, recovering...");
          hls.recoverMediaError();
          break;
        default:
          console.error("❌ Fatal error:", data.details);
          break;
      }
    }
  });

} else if (audio.canPlayType('application/vnd.apple.mpegurl')) {
  audio.src = audioSrc;
  console.log("✅ Safari native HLS");
  // Safari handles ABR automatically, no quality selector needed
  if (qualitySelector) {
    qualitySelector.style.display = 'none';
  }
}

// Populate quality selector dropdown
function populateQualitySelector(levels) {
  if (!qualitySelector) return;
  
  // Clear existing options
  qualitySelector.innerHTML = '';
  
  // Add Auto option
  const autoOption = document.createElement('option');
  autoOption.value = '-1';
  autoOption.textContent = 'Auto';
  autoOption.selected = true;
  qualitySelector.appendChild(autoOption);
  
  // Add quality levels
  levels.forEach((level, index) => {
    const option = document.createElement('option');
    option.value = index;
    const bitrate = Math.round(level.bitrate / 1000);
    option.textContent = `${bitrate}kbps`;
    qualitySelector.appendChild(option);
  });
  
  console.log(`📋 Quality selector populated with ${levels.length} levels`);
}

// Update quality selector to reflect current level
function updateQualityDisplay(levelIndex) {
  if (!qualitySelector) return;
  
  // If auto mode, don't change selector value
  if (hls.currentLevel === -1) {
    qualitySelector.value = '-1';
  } else {
    qualitySelector.value = levelIndex.toString();
  }
}

// Handle quality selector change
if (qualitySelector) {
  qualitySelector.addEventListener('change', (e) => {
    if (!hls) return;
    
    const selectedLevel = parseInt(e.target.value);
    
    if (selectedLevel === -1) {
      // Auto mode
      hls.currentLevel = -1;
      console.log("🔄 Switched to Auto quality");
    } else {
      // Manual quality selection
      hls.currentLevel = selectedLevel;
      const level = hls.levels[selectedLevel];
      const bitrate = Math.round(level.bitrate / 1000);
      console.log(`🔄 Manually selected: ${bitrate}kbps`);
    }
  });
}

// Text track setup
audio.addEventListener('loadedmetadata', () => {
  console.log("✅ Media loaded");
  setTimeout(setupTextTracks, 500);
});

function setupTextTracks() {
  const textTracks = audio.textTracks;
  console.log(`📝 Found ${textTracks.length} text track(s)`);

  if (textTracks.length === 0) {
    lyricsDisplay.innerHTML = '<div class="lyrics-line">No lyrics available</div>';
    return;
  }

  for (let i = 0; i < textTracks.length; i++) {
    const track = textTracks[i];
    track.mode = 'hidden';
    
    if (track.cues && track.cues.length > 0) {
      loadAllCues(track);
    } else {
      track.addEventListener('load', () => {
        if (track.cues && track.cues.length > 0) {
          loadAllCues(track);
        }
      });

      const checkCues = setInterval(() => {
        if (track.cues && track.cues.length > 0) {
          clearInterval(checkCues);
          loadAllCues(track);
        }
      }, 500);

      setTimeout(() => clearInterval(checkCues), 10000);
    }
  }
}

function loadAllCues(track) {
  console.log(`📥 Loading ${track.cues.length} cues into memory`);
  allCues = [];
  
  for (let i = 0; i < track.cues.length; i++) {
    const cue = track.cues[i];
    const parsedCue = parseCue(cue, i);
    if (parsedCue.words.length > 0) {
      allCues.push(parsedCue);
    }
  }
  
  console.log(`✅ Loaded ${allCues.length} lyric cues`);
  lyricsDisplay.innerHTML = '<div class="lyrics-line lyrics-placeholder">♪ Play to see lyrics ♪</div>';
}

function parseCue(cue, cueIndex) {
  const parsed = {
    index: cueIndex,
    start: cue.startTime,
    end: cue.endTime,
    words: []
  };

  const text = cue.text || '';
  const wordRegex = /<(\d{2}:\d{2}:\d{2}\.\d{3})>([^<]+)/g;
  let match;
  let foundWords = false;

  while ((match = wordRegex.exec(text)) !== null) {
    foundWords = true;
    const wordText = match[2].trim();
    if (wordText) {
      parsed.words.push({
        time: timeToSeconds(match[1]),
        text: wordText
      });
    }
  }

  if (!foundWords && text.trim()) {
    text.split(/\s+/).filter(w => w.trim()).forEach(word => {
      parsed.words.push({ 
        time: parsed.start, 
        text: word.trim() 
      });
    });
  }

  return parsed;
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

function appendLyricsIfNeeded(curTime) {
  if (allCues.length === 0) return;

  allCues.forEach((cue, index) => {
    const shouldDisplay = curTime >= (cue.start - 2);
    const alreadyDisplayed = displayedCues.has(index);
    
    if (shouldDisplay && !alreadyDisplayed) {
      appendLyricLine(cue, index);
      displayedCues.add(index);
    }
  });
}

function appendLyricLine(cue, index) {
  const placeholder = lyricsDisplay.querySelector('.lyrics-placeholder');
  if (placeholder) {
    placeholder.remove();
  }

  const lineDiv = document.createElement('div');
  lineDiv.className = 'lyrics-line';
  lineDiv.id = `cue-${index}`;
  lineDiv.style.opacity = '0';
  lineDiv.style.transform = 'translateY(20px)';

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

  requestAnimationFrame(() => {
    lineDiv.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
    lineDiv.style.opacity = '1';
    lineDiv.style.transform = 'translateY(0)';
  });
}

function syncLyrics(curTime) {
  if (allCues.length === 0) return;

  appendLyricsIfNeeded(curTime);

  let activeIndex = -1;
  for (let i = 0; i < allCues.length; i++) {
    if (displayedCues.has(i)) {
      const cue = allCues[i];
      if (curTime >= cue.start && curTime <= cue.end + 0.1) {
        activeIndex = i;
        break;
      }
    }
  }

  if (activeIndex !== currentLyricIndex) {
    
    if (previousLyricIndex !== -1 && previousLyricIndex !== activeIndex) {
      const prevElement = document.getElementById(`cue-${previousLyricIndex}`);
      if (prevElement) {
        const prevWords = prevElement.querySelectorAll('.word');
        prevWords.forEach(word => {
          word.classList.add('active');
        });
        
        setTimeout(() => {
          prevElement.classList.remove('active');
          prevElement.classList.add('played');
          prevWords.forEach(w => w.classList.remove('active'));
        }, 100);
      }
    }

    if (activeIndex !== -1) {
      const activeElement = document.getElementById(`cue-${activeIndex}`);
      if (activeElement) {
        activeElement.classList.add('active');
        activeElement.classList.remove('played');
        activeElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }

    previousLyricIndex = currentLyricIndex;
    currentLyricIndex = activeIndex;
  }

  if (activeIndex !== -1) {
    const cue = allCues[activeIndex];
    const activeElement = document.getElementById(`cue-${activeIndex}`);

    if (activeElement) {
      const words = activeElement.querySelectorAll('.word');
      
      cue.words.forEach((wordCue, i) => {
        if (i < words.length) {
          const shouldHighlight = curTime >= wordCue.time || 
                                 (i === cue.words.length - 1 && curTime >= wordCue.time - 0.1);
          
          words[i].classList.toggle('active', shouldHighlight);
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
  const newTime = (seekBar.value / 100) * audio.duration;
  audio.currentTime = newTime;
  
  if (allCues.length > 0) {
    allCues.forEach((cue, index) => {
      if (newTime >= (cue.start - 2) && !displayedCues.has(index)) {
        appendLyricLine(cue, index);
        displayedCues.add(index);
      }
    });
  }
});

function formatTime(seconds) {
  if (isNaN(seconds)) return '0:00';
  const min = Math.floor(seconds / 60);
  const sec = Math.floor(seconds % 60);
  return `${min}:${sec.toString().padStart(2, '0')}`;
}

console.log("🎵 Music player with ABR support initialized");