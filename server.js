const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const ftpLib = require('basic-ftp');
const SftpClient = require('ssh2-sftp-client');
const nodemailer = require('nodemailer');
const oracledb = require('oracledb');
oracledb.thin = true; // 纯 JS 模式，无需 Oracle Instant Client
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
// 禁止静态文件缓存，确保前端始终拿到最新版本
app.use((req, res, next) => {
  if (req.path.endsWith('.html') || req.path === '/') {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
  }
  next();
});
app.use(express.static(path.join(__dirname, 'public')));

const CONFIG_FILE = path.join(__dirname, 'config.json');
const UPLOAD_RECORDS_FILE = path.join(__dirname, 'uploaded-records.json');
const MONITOR_SNAPSHOT_FILE = path.join(__dirname, 'monitor-snapshots.json');
const REUPLOAD_RECORDS_FILE = path.join(__dirname, 'reupload-records.json');
const RECONCILE_VERIFIED_FILE = path.join(__dirname, 'reconcile-verified.json');

// 默认配置
let config = {
  ftp: {
    protocol: 'sftp',  // 'ftp' | 'sftp'
    host: '',
    port: 22,
    user: '',
    password: '',
    secure: false,       // 仅 FTP/FTPS 时有效
    privateKey: '',      // SFTP 密钥登录（可选）
    passphrase: ''       // 密钥口令（可选）
  },
  folders: [],
  // 上传任务配置
  uploadFolders: [],     // [{ localPath, remotePath, filePattern, enabled, deleteAfterUpload }]
  uploadEnabled: false,  // 是否启用上传任务
  uploadSchedule: {      // 上传独立调度（留空时与下载同步执行）
    type: 'same',        // 'same'=与下载同步 | 'interval' | 'cron'
    interval: 10,
    cronExpr: '*/10 * * * *'
  },
  schedule: {
    type: 'interval',
    interval: 10,
    cronExpr: '*/10 * * * *'
  },
  localDir: '',
  useDateFolder: true,
  dateFolderFormat: 'YYYY-MM-DD',
  deleteAfterDownload: false,
  overwrite: true,
  // 文件监控 + 邮件通知
  mail: {
    host: '',
    port: 465,
    secure: true,
    user: '',
    password: '',
    to: ''
  },
  monitors: [],          // [{ id, remotePath, filePattern, enabled }]
  monitorEnabled: false, // 是否启用监控调度
  monitorInterval: 5,    // 监控扫描间隔（分钟）
  // 归档报文对账 + 自动重传
  // 注意：默认不含任何凭据。真实配置请通过 config.json（已被 .gitignore 忽略，不会上传）
  // 或环境变量（ORACLE_HOST / ORACLE_USER / ORACLE_PASS / ORACLE_SERVICE）注入，切勿提交到仓库。
  oracle: {
    host: '',
    port: 1521,
    serviceName: '',        // 实际连接用的服务名（如 PHYDB）
    user: '',
    password: '',
    enabled: false          // 是否启用对账功能
  },
  reconcile: {
    enabled: false,         // 是否启用定时自动对账重传
    interval: 30,           // 对账间隔（分钟）
    windowDays: 7,          // 扫描最近 N 天的归档
    autoReupload: true      // 检出不一致是否自动重传
  }
};

let state = {
  running: false,
  lastRun: null,
  lastStatus: '',
  logs: [],
  cronJob: null,
  intervalTimer: null,       // 秒级 interval 定时器（下载）
  uploadCronJob: null,       // 上传独立调度 cron
  uploadIntervalTimer: null, // 上传独立调度 setInterval
  nextRun: null,
  // 连接复用
  persistentSftp: null,      // 常驻 SFTP 客户端
  persistentFtp: null,       // 常驻 FTP 客户端
  connFingerprint: '',       // 当前连接对应的配置摘要（变更时断开重建）
  // 文件监控
  monitorCronJob: null,
  monitorIntervalTimer: null,
  monitorRunning: false,
  lastMonitorRun: null,
  // 归档报文对账
  reconcileCronJob: null,
  reconcileIntervalTimer: null,
  reconcileRunning: false,
  lastReconcileRun: null,
  lastReconcileSkipped: 0
};

// ========== 工具函数 ==========

/** 判断 SFTP 错误是否为连接断开类（触发自动重连） */
function isSftpConnectionError(err) {
  if (!err || !err.message) return false;
  const msg = err.message;
  return /EPIPE|broken pipe|timed out|not connected|no sftp connection|connection lost|closed/i.test(msg);
}

// 文件被占用（下载/上传同时操作同一文件时的竞态）
function isFileLockError(err) {
  if (!err || !err.message) return false;
  const msg = err.message;
  return /EPERM|EACCES|EBUSY|operation not permitted|permission denied/i.test(msg);
}

// 带退避的重试（用于文件被占用场景）
async function retryWithBackoff(fn, maxRetries = 5, baseDelay = 800) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (!isFileLockError(e) || attempt === maxRetries - 1) throw e;
      const delay = baseDelay * Math.pow(2, attempt);
      addLog('warn', `   文件被占用，${delay}ms 后重试 (${attempt + 1}/${maxRetries})`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

// ========== 上传记录（防重复上传） ==========

let uploadRecords = {};  // { fileKey: { size, mtime, uploadedAt } }

function loadUploadRecords() {
  try {
    if (fs.existsSync(UPLOAD_RECORDS_FILE)) {
      uploadRecords = JSON.parse(fs.readFileSync(UPLOAD_RECORDS_FILE, 'utf-8'));
    }
  } catch (e) {
    uploadRecords = {};
  }
}

function saveUploadRecords() {
  try {
    fs.writeFileSync(UPLOAD_RECORDS_FILE, JSON.stringify(uploadRecords, null, 2), 'utf-8');
  } catch (_) {}
}

/** 根据本地文件路径生成唯一键（去掉盘符大小写差异） */
function fileKey(localPath) {
  return localPath.replace(/\\/g, '/').toLowerCase();
}

/** 文件是否已上传过（检查路径+大小+修改时间是否一致） */
function isAlreadyUploaded(localPath) {
  const key = fileKey(localPath);
  const rec = uploadRecords[key];
  if (!rec) return false;
  try {
    const stat = fs.statSync(localPath);
    return rec.size === stat.size && rec.mtime === stat.mtimeMs;
  } catch (_) {
    return false;
  }
}

/** 标记文件已上传成功 */
function markUploaded(localPath) {
  const key = fileKey(localPath);
  try {
    const stat = fs.statSync(localPath);
    uploadRecords[key] = { size: stat.size, mtime: stat.mtimeMs, uploadedAt: Date.now() };
    saveUploadRecords();
  } catch (_) {}
}

/** 清理已不存在的文件的上传记录（定期维护，避免无限增长） */
function pruneUploadRecords() {
  let pruned = 0;
  for (const key of Object.keys(uploadRecords)) {
    const winPath = key.replace(/\//g, '\\');
    if (!fs.existsSync(winPath) && !fs.existsSync(key)) {
      delete uploadRecords[key];
      pruned++;
    }
  }
  if (pruned > 0) saveUploadRecords();
}

function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      const saved = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf-8'));
      config = deepMerge(config, saved);
    }
  } catch (e) {
    addLog('warn', '加载配置失败: ' + e.message);
  }
}

function deepMerge(target, source) {
  const result = Object.assign({}, target);
  for (const key of Object.keys(source)) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      result[key] = deepMerge(target[key] || {}, source[key]);
    } else {
      result[key] = source[key];
    }
  }
  return result;
}

function saveConfig() {
  try {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2), 'utf-8');
  } catch (e) {
    addLog('error', '保存配置失败: ' + e.message);
  }
}

// ========== 环境变量覆盖（无头 / 容器 / Linux 部署用） ==========
// 允许通过环境变量注入配置，避免在仓库中提交任何凭据。
// 仅在变量存在时才覆盖，优先级：环境变量 > config.json > 默认值。
function applyEnvOverrides() {
  const env = process.env;
  const setIf = (val, fn) => { if (val !== undefined && val !== '') fn(val); };

  // FTP / SFTP 连接
  setIf(env.FTP_PROTOCOL, v => config.ftp.protocol = v);
  setIf(env.FTP_HOST, v => config.ftp.host = v);
  setIf(env.FTP_PORT, v => config.ftp.port = parseInt(v, 10) || 22);
  setIf(env.FTP_USER, v => config.ftp.user = v);
  setIf(env.FTP_PASS, v => config.ftp.password = v);
  setIf(env.FTP_SECURE, v => config.ftp.secure = /^(1|true|yes)$/i.test(v));
  setIf(env.FTP_PRIVATE_KEY, v => config.ftp.privateKey = v);
  setIf(env.FTP_PASSPHRASE, v => config.ftp.passphrase = v);

  // 本地归档根目录（跨平台关键：Linux 上不要写死 Windows 盘符路径）
  setIf(env.LOCAL_DIR, v => config.localDir = v);

  // Oracle（归档报文对账）
  setIf(env.ORACLE_HOST, v => config.oracle.host = v);
  setIf(env.ORACLE_PORT, v => config.oracle.port = parseInt(v, 10) || 1521);
  setIf(env.ORACLE_SERVICE, v => config.oracle.serviceName = v);
  setIf(env.ORACLE_USER, v => config.oracle.user = v);
  setIf(env.ORACLE_PASS, v => config.oracle.password = v);
  setIf(env.ORACLE_ENABLED, v => config.oracle.enabled = /^(1|true|yes)$/i.test(v));

  // 自动对账开关
  setIf(env.RECONCILE_ENABLED, v => config.reconcile.enabled = /^(1|true|yes)$/i.test(v));
  setIf(env.RECONCILE_INTERVAL, v => config.reconcile.interval = parseInt(v, 10) || 30);
  setIf(env.RECONCILE_WINDOW_DAYS, v => config.reconcile.windowDays = parseInt(v, 10) || 7);
}

// config.json 不存在时，用当前（默认 / 环境变量）配置初始化一个，方便首次启动后通过界面保存
function ensureConfigFile() {
  try {
    if (!fs.existsSync(CONFIG_FILE)) {
      fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2), 'utf-8');
      addLog('info', '未检测到 config.json，已用默认配置初始化（请通过界面或环境变量补全凭据）');
    }
  } catch (_) {}
}

function addLog(level, message) {
  const entry = {
    time: new Date().toLocaleString('zh-CN'),
    ts: Date.now(),
    level,
    message
  };
  state.logs.unshift(entry);
  if (state.logs.length > 1000) state.logs.pop();
  console.log(`[${level.toUpperCase()}] ${entry.time} - ${message}`);
}

function getDateFolder() {
  const now = new Date();
  const fmt = config.dateFolderFormat || 'YYYY-MM-DD';
  const pad = n => String(n).padStart(2, '0');
  return fmt
    .replace('YYYY', now.getFullYear())
    .replace('MM', pad(now.getMonth() + 1))
    .replace('DD', pad(now.getDate()))
    .replace('HH', pad(now.getHours()));
}

function patternToRegex(pattern) {
  if (!pattern || pattern === '*') return /.*/;
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');
  return new RegExp('^' + escaped + '$', 'i');
}

function formatSize(bytes) {
  if (!bytes) return '?';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1024 / 1024).toFixed(2) + ' MB';
}

// ========== SFTP 操作封装 ==========

async function sftpConnect(ftpConf) {
  const sftp = new SftpClient();
  const connectOpts = {
    host: ftpConf.host,
    port: ftpConf.port || 22,
    username: ftpConf.user || '',
    password: ftpConf.password || '',
    readyTimeout: 30000,
    retries: 2,
    retry_factor: 2,
    retry_minTimeout: 2000,
    // SSH 保活：每15秒发一次心跳，防止服务器踢掉空闲连接
    keepaliveInterval: 15000,
    keepaliveCountMax: 3,
    // 减少内部超时
    timeout: 30000
  };
  if (ftpConf.privateKey) {
    connectOpts.privateKey = fs.readFileSync(ftpConf.privateKey);
    if (ftpConf.passphrase) connectOpts.passphrase = ftpConf.passphrase;
  }
  await sftp.connect(connectOpts);
  return sftp;
}

// ========== 连接复用（持久连接） ==========

function getConnFingerprint() {
  const f = config.ftp;
  return `${f.protocol || 'sftp'}|${f.host}|${f.port || 22}|${f.user}`;
}

async function ensureSftpConnected() {
  const fp = getConnFingerprint();
  // 配置变更 → 断开旧连接
  if (state.persistentSftp && state.connFingerprint !== fp) {
    addLog('info', 'SFTP 配置变更，断开旧连接并重建');
    try { await state.persistentSftp.end(); } catch (_) {}
    state.persistentSftp = null;
  }
  // 已有连接直接复用
  if (state.persistentSftp) return state.persistentSftp;
  // 新建立连接
  state.persistentSftp = await sftpConnect(config.ftp);
  state.connFingerprint = fp;
  addLog('info', `SFTP 持久连接已建立: ${config.ftp.host}:${config.ftp.port || 22}`);
  return state.persistentSftp;
}

async function ensureFtpConnected() {
  const fp = getConnFingerprint();
  if (state.persistentFtp && state.connFingerprint !== fp) {
    try { state.persistentFtp.close(); } catch (_) {}
    state.persistentFtp = null;
  }
  if (state.persistentFtp) return state.persistentFtp;
  state.persistentFtp = await ftpConnect(config.ftp);
  state.connFingerprint = fp;
  addLog('info', `FTP 持久连接已建立: ${config.ftp.host}:${config.ftp.port || 21}`);
  return state.persistentFtp;
}

function disconnectAll() {
  if (state.persistentSftp) {
    try { state.persistentSftp.end(); } catch (_) {}
    state.persistentSftp = null;
  }
  if (state.persistentFtp) {
    try { state.persistentFtp.close(); } catch (_) {}
    state.persistentFtp = null;
  }
  state.connFingerprint = '';
}

async function sftpListDir(ftpConf, remotePath) {
  const sftp = await sftpConnect(ftpConf);
  try {
    const list = await sftp.list(remotePath || '/');
    await sftp.end();
    return { ok: true, list };
  } catch (e) {
    try { await sftp.end(); } catch (_) {}
    throw e;
  }
}

async function sftpDownloadFile(sftp, remotePath, localPath) {
  await sftp.fastGet(remotePath, localPath);
}

// ========== FTP 操作封装 ==========

async function ftpConnect(ftpConf) {
  const client = new ftpLib.Client();
  client.ftp.verbose = false;
  await client.access({
    host: ftpConf.host,
    port: ftpConf.port || 21,
    user: ftpConf.user || 'anonymous',
    password: ftpConf.password || '',
    secure: ftpConf.secure || false
  });
  return client;
}

// ========== 核心抓取逻辑 ==========

async function fetchFiles() {
  const { ftp: ftpConf, folders, localDir, useDateFolder, deleteAfterDownload, overwrite } = config;
  // 协议判断：优先用配置，兜底用端口（22→sftp，21→ftp）
  let protocol = ftpConf.protocol;
  if (!protocol) protocol = (parseInt(ftpConf.port) === 21) ? 'ftp' : 'sftp';
  if (protocol === 'ftp' && parseInt(ftpConf.port) === 22) protocol = 'sftp';

  if (!ftpConf.host) {
    addLog('warn', '主机未配置，跳过本次抓取');
    return { success: false, message: '主机未配置' };
  }
  if (!localDir) {
    addLog('warn', '本地目录未配置，跳过本次抓取');
    return { success: false, message: '本地目录未配置' };
  }

  const enabledFolders = (folders || []).filter(f => f.enabled && f.remotePath);
  if (enabledFolders.length === 0) {
    addLog('warn', '没有启用的远程文件夹配置，跳过本次抓取');
    return { success: false, message: '没有启用的文件夹' };
  }

  const dateFolder = useDateFolder ? getDateFolder() : '';
  addLog('info', `开始抓取 [${protocol.toUpperCase()}]，目标文件夹数: ${enabledFolders.length}${dateFolder ? '，归档目录: ' + dateFolder : ''}`);

  let totalDownloaded = 0, totalSkipped = 0, totalFailed = 0;

  // ---- SFTP ----
  if (protocol === 'sftp') {
    let sftp;
    try {
      sftp = await ensureSftpConnected();
    } catch (e) {
      addLog('error', `SFTP 连接失败: ${e.message}`);
      state.lastStatus = 'error';
      return { success: false, message: e.message };
    }

    for (const folder of enabledFolders) {
      const remotePath = folder.remotePath;
      const pattern = folder.filePattern || '*';
      const regex = patternToRegex(pattern);

      addLog('info', `── 处理远程目录: ${remotePath}（模式: ${pattern}）`);

      let fileList, dirStat;
      try {
        [fileList, dirStat] = await Promise.all([
          sftp.list(remotePath),
          sftp.stat(remotePath).catch(() => null)
        ]);
      } catch (e) {
        // 连接断开 → 断开旧连接，重建后重试
        if (isSftpConnectionError(e)) {
          addLog('warn', `SFTP 连接断开，自动重连...`);
          disconnectAll();
          try {
            sftp = await ensureSftpConnected();
            addLog('info', `SFTP 重连成功`);
            fileList = await sftp.list(remotePath);
            dirStat = await sftp.stat(remotePath).catch(() => null);
          } catch (e2) {
            addLog('error', `重连后列举 ${remotePath} 仍失败: ${e2.message}`);
            continue;
          }
        } else {
          addLog('error', `列举 ${remotePath} 失败: ${e.message}`);
          continue;
        }
      }

      // 记录目录元信息（修改时间），帮助判断文件是否被其他服务抢先消费
      const dirMtime = dirStat ? new Date(dirStat.modifyTime).toLocaleString('zh-CN') : '未知';
      addLog('info', `  目录信息: ${fileList.length} 个条目，目录修改时间 ${dirMtime}`);
      // 把所有条目名字打印出来（最多前 20 个），方便排查
      const fileNames = fileList.map(f => `${f.name}(${f.type},${formatSize(f.size)})`);
      if (fileNames.length > 0) {
        addLog('info', `  条目列表: ${fileNames.slice(0, 20).join(' | ')}${fileNames.length > 20 ? ' ...还有' + (fileNames.length - 20) + '个' : ''}`);
      }
      const files = fileList.filter(item => item.type === '-' && regex.test(item.name));
      addLog('info', `  匹配文件: ${files.length} 个`);

      if (files.length === 0) {
        // 目录最近被修改过（5分钟内），但列表为空 → 文件可能被其他服务抢先消费
        if (dirStat) {
          const deltaMs = Date.now() - dirStat.modifyTime;
          const deltaSec = Math.round(deltaMs / 1000);
          if (deltaSec < 300 && deltaSec > 0) {
            addLog('warn', `  目录 ${deltaSec} 秒前刚被修改过但当前无文件 — 可能已被其他服务抢先取走`);
          }
        }
        addLog('info', '  没有匹配文件，跳过');
        continue;
      }

      const remoteFolderName = path.posix.basename(remotePath) || 'root';
      let destDir = localDir;
      if (dateFolder) destDir = path.join(destDir, dateFolder);
      destDir = path.join(destDir, remoteFolderName);

      try {
        if (!fs.existsSync(destDir)) {
          fs.mkdirSync(destDir, { recursive: true });
          addLog('info', `  创建目录: ${destDir}`);
        }
      } catch (e) {
        addLog('error', `  创建目录失败: ${e.message}`);
        continue;
      }

      let downloaded = 0, skipped = 0, failed = 0;

      for (const file of files) {
        const remoteFilePath = remotePath.replace(/\/+$/, '') + '/' + file.name;
        const localFilePath = path.join(destDir, file.name);

        if (!overwrite && fs.existsSync(localFilePath)) {
          addLog('info', `  跳过（已存在）: ${file.name}`);
          skipped++;
          continue;
        }

        try {
          addLog('info', `  下载: ${file.name} (${formatSize(file.size)})`);
          await sftp.fastGet(remoteFilePath, localFilePath);
          downloaded++;
          addLog('info', `  ✓ 完成: ${file.name}`);

          if (deleteAfterDownload) {
            await sftp.delete(remoteFilePath);
            addLog('info', `  ✓ 已删除远程文件: ${file.name}`);
          }
        } catch (e) {
          // 连接断开 → 断开重建，重试一次
          const isPipe = isSftpConnectionError(e);
          if (isPipe) {
            addLog('warn', `  连接断开，重连重试: ${file.name}`);
            disconnectAll();
            try {
              sftp = await ensureSftpConnected();
              await sftp.fastGet(remoteFilePath, localFilePath);
              downloaded++;
              addLog('info', `  ✓ 重试成功: ${file.name}`);
              continue;
            } catch (e2) {
              addLog('error', `  重试仍失败: ${file.name} - ${e2.message}`);
            }
          }
          failed++;
          addLog('error', `  ✗ 失败: ${file.name} - ${e.message}`);
        }
      }

      addLog('info', `  目录 ${remoteFolderName} 完成: 下载 ${downloaded}, 跳过 ${skipped}, 失败 ${failed}`);
      totalDownloaded += downloaded;
      totalSkipped += skipped;
      totalFailed += failed;
    }

  // ---- FTP/FTPS ----
  } else {
    let client;
    try {
      client = await ensureFtpConnected();
    } catch (e) {
      addLog('error', `FTP 连接失败: ${e.message}`);
      state.lastStatus = 'error';
      return { success: false, message: e.message };
    }

    for (const folder of enabledFolders) {
        const remotePath = folder.remotePath;
        const pattern = folder.filePattern || '*';
        const regex = patternToRegex(pattern);

        addLog('info', `── 处理远程目录: ${remotePath}（模式: ${pattern}）`);

        try { await client.cd(remotePath); } catch (e) {
          addLog('error', `切换到 ${remotePath} 失败: ${e.message}`);
          continue;
        }

        let fileList;
        try { fileList = await client.list(); } catch (e) {
          addLog('error', `列举失败: ${e.message}`);
          continue;
        }

        const files = fileList.filter(item => item.type === ftpLib.FileType.File && regex.test(item.name));
        const ftpNames = fileList.map(f => `${f.name}(${f.type === ftpLib.FileType.Directory ? 'd' : 'f'},${formatSize(f.size)})`);
        addLog('info', `  发现 ${fileList.length} 个条目，匹配 ${files.length} 个文件`);
        if (ftpNames.length > 0 && files.length === 0) {
          addLog('info', `  条目列表: ${ftpNames.slice(0, 20).join(' | ')}${ftpNames.length > 20 ? ' ...还有' + (ftpNames.length - 20) + '个' : ''}`);
        }

        if (files.length === 0) { addLog('info', '  没有匹配文件，跳过'); continue; }

        const remoteFolderName = path.basename(remotePath) || 'root';
        let destDir = localDir;
        if (dateFolder) destDir = path.join(destDir, dateFolder);
        destDir = path.join(destDir, remoteFolderName);

        try {
          if (!fs.existsSync(destDir)) {
            fs.mkdirSync(destDir, { recursive: true });
            addLog('info', `  创建目录: ${destDir}`);
          }
        } catch (e) {
          addLog('error', `  创建目录失败: ${e.message}`);
          continue;
        }

        let downloaded = 0, skipped = 0, failed = 0;

        for (const file of files) {
          const localFilePath = path.join(destDir, file.name);
          if (!overwrite && fs.existsSync(localFilePath)) {
            addLog('info', `  跳过（已存在）: ${file.name}`);
            skipped++;
            continue;
          }
          try {
            addLog('info', `  下载: ${file.name} (${formatSize(file.size)})`);
            await client.downloadTo(localFilePath, file.name);
            downloaded++;
            addLog('info', `  ✓ 完成: ${file.name}`);
            if (deleteAfterDownload) {
              await client.remove(file.name);
              addLog('info', `  ✓ 已删除远程文件: ${file.name}`);
            }
          } catch (e) {
            failed++;
            addLog('error', `  ✗ 失败: ${file.name} - ${e.message}`);
          }
        }

        addLog('info', `  目录 ${remoteFolderName} 完成: 下载 ${downloaded}, 跳过 ${skipped}, 失败 ${failed}`);
        totalDownloaded += downloaded;
        totalSkipped += skipped;
        totalFailed += failed;
      }
  }

  const summary = `本次抓取结束 ── 总计下载: ${totalDownloaded}, 跳过: ${totalSkipped}, 失败: ${totalFailed}`;
  addLog('info', summary);
  state.lastRun = new Date().toLocaleString('zh-CN');
  state.lastStatus = totalFailed > 0 ? 'warn' : 'success';
  return { success: true, downloaded: totalDownloaded, skipped: totalSkipped, failed: totalFailed, message: summary };
}

// ========== 核心上传逻辑 ==========

// 展开 {date} 占位符为指定的日期列表
function expandDatePaths(localPathTemplate, scanDates) {
  // 兼容旧格式：scanDates 为数字（天数）
  if (typeof scanDates === 'number' && scanDates > 0) {
    const paths = [];
    const today = new Date();
    for (let i = 0; i < scanDates; i++) {
      const d = new Date(today);
      d.setDate(d.getDate() - i);
      const dateStr = d.getFullYear() + '-' +
        String(d.getMonth() + 1).padStart(2, '0') + '-' +
        String(d.getDate()).padStart(2, '0');
      paths.push({ localPath: localPathTemplate.replace(/\{date\}/g, dateStr), dateLabel: dateStr });
    }
    return paths;
  }
  // 新格式：scanDates 为日期字符串数组
  if (!Array.isArray(scanDates) || scanDates.length === 0) {
    return [{ localPath: localPathTemplate, dateLabel: '' }];
  }
  return scanDates.map(ds => ({
    localPath: localPathTemplate.replace(/\{date\}/g, ds),
    dateLabel: ds
  }));
}

async function uploadFiles() {
  const { ftp: ftpConf, uploadFolders } = config;
  let protocol = ftpConf.protocol || 'sftp';
  if (protocol === 'ftp' && parseInt(ftpConf.port) === 22) protocol = 'sftp';

  if (!ftpConf.host) {
    addLog('warn', '[上传] 主机未配置，跳过');
    return { success: false, message: '主机未配置' };
  }

  const enabledFolders = (uploadFolders || []).filter(f => f.enabled && f.localPath && f.remotePath);
  if (enabledFolders.length === 0) {
    addLog('warn', '[上传] 没有启用的上传文件夹配置，跳过');
    return { success: false, message: '没有启用的上传文件夹' };
  }

  addLog('info', `[上传] 开始上传 [${protocol.toUpperCase()}]，目标文件夹数: ${enabledFolders.length}`);

  // 偶尔清理一下无效记录
  if (Math.random() < 0.1) pruneUploadRecords();

  let totalUploaded = 0, totalSkipped = 0, totalFailed = 0;

  // ---- SFTP 上传 ----
  if (protocol === 'sftp') {
    let sftp;
    try {
      sftp = await ensureSftpConnected();
    } catch (e) {
      addLog('error', `[上传] SFTP 连接失败: ${e.message}`);
      return { success: false, message: e.message };
    }

    for (const folder of enabledFolders) {
      const { localPath, remotePath, filePattern, deleteAfterUpload, scanDates = [], dateScanDays = 0, skipUploaded = false } = folder;
      const regex = patternToRegex(filePattern || '*');

      // 按日期展开路径（优先使用新格式 scanDates 数组，兼容旧 dateScanDays 数字）
      const effectiveDates = (Array.isArray(scanDates) && scanDates.length > 0)
        ? scanDates
        : ((typeof dateScanDays === 'number' && dateScanDays > 0) ? dateScanDays : 0);
      const datePaths = effectiveDates
        ? expandDatePaths(localPath, effectiveDates)
        : [{ localPath, dateLabel: '' }];

      for (const dp of datePaths) {
        const dateTag = dp.dateLabel ? ` [${dp.dateLabel}]` : '';

        addLog('info', `[上传] ── 本地目录: ${dp.localPath} → 远程: ${remotePath}${dateTag}`);

        // 检查本地目录
        if (!fs.existsSync(dp.localPath)) {
          addLog('warn', `[上传]   本地目录不存在: ${dp.localPath}，跳过`);
          continue;
        }

        // 列出本地文件
        let localFiles;
        try {
          localFiles = fs.readdirSync(dp.localPath)
            .filter(name => {
              const full = path.join(dp.localPath, name);
              return fs.statSync(full).isFile() && regex.test(name);
            });
        } catch (e) {
          addLog('error', `[上传]   读取本地目录失败: ${e.message}`);
          continue;
        }

        addLog('info', `[上传]   本地匹配文件: ${localFiles.length} 个`);

        if (localFiles.length === 0) {
          addLog('info', '[上传]   没有匹配文件，跳过');
          continue;
        }

        // 确保远程目录存在
        try {
          await sftp.mkdir(remotePath, true);
        } catch (_) {
          // mkdir 如果目录已存在通常会抛出，忽略
        }

        let uploaded = 0, skipped = 0, failed = 0;

        for (const name of localFiles) {
          const localFilePath = path.join(dp.localPath, name);
          const remoteFilePath = remotePath.replace(/\/+$/, '') + '/' + name;

          // 仅当勾选「跳过已上传」时才检查上传记录
          if (skipUploaded && isAlreadyUploaded(localFilePath)) {
            addLog('info', `[上传]   跳过（已上传）: ${name}`);
            skipped++;
            continue;
          }

          try {
            const stat = fs.statSync(localFilePath);
            addLog('info', `[上传]   上传: ${name} (${formatSize(stat.size)})`);
            // 用退避重试包裹，防止文件正被下载进程写入导致 EPERM
            await retryWithBackoff(() => sftp.fastPut(localFilePath, remoteFilePath));
            markUploaded(localFilePath);
            uploaded++;
            addLog('info', `[上传]   ✓ 完成: ${name}`);

            if (deleteAfterUpload) {
              fs.unlinkSync(localFilePath);
              addLog('info', `[上传]   ✓ 已删除本地文件: ${name}`);
            }
          } catch (e) {
            // 文件被占用（下载进程正在写入）→ 已通过 retryWithBackoff 重试，仍失败则记录
            if (isFileLockError(e)) {
              addLog('error', `[上传]   ✗ 被占用（重试耗尽）: ${name} - ${e.message}`);
            }
            // 连接断开 → 重连重试
            else if (isSftpConnectionError(e)) {
              addLog('warn', `[上传]   连接断开，重连重试: ${name}`);
              disconnectAll();
              try {
                sftp = await ensureSftpConnected();
                await retryWithBackoff(() => sftp.fastPut(localFilePath, remoteFilePath));
                markUploaded(localFilePath);
                uploaded++;
                addLog('info', `[上传]   ✓ 重试成功: ${name}`);
                if (deleteAfterUpload) {
                  fs.unlinkSync(localFilePath);
                  addLog('info', `[上传]   ✓ 已删除本地文件: ${name}`);
                }
                continue;
              } catch (e2) {
                addLog('error', `[上传]   重试仍失败: ${name} - ${e2.message}`);
              }
            } else {
              addLog('error', `[上传]   ✗ 失败: ${name} - ${e.message}`);
            }
            failed++;
          }
        }

        addLog('info', `[上传]   目录完成: 上传 ${uploaded}, 跳过 ${skipped}, 失败 ${failed}`);
        totalUploaded += uploaded;
        totalSkipped += skipped;
        totalFailed += failed;
      }
    }

  // ---- FTP/FTPS 上传 ----
  } else {
    let client;
    try {
      client = await ensureFtpConnected();
    } catch (e) {
      addLog('error', `[上传] FTP 连接失败: ${e.message}`);
      return { success: false, message: e.message };
    }

    for (const folder of enabledFolders) {
      const { localPath, remotePath, filePattern, deleteAfterUpload, scanDates = [], dateScanDays = 0, skipUploaded = false } = folder;
      const regex = patternToRegex(filePattern || '*');

      // 按日期展开路径（优先使用新格式 scanDates 数组，兼容旧 dateScanDays 数字）
      const effectiveDates = (Array.isArray(scanDates) && scanDates.length > 0)
        ? scanDates
        : ((typeof dateScanDays === 'number' && dateScanDays > 0) ? dateScanDays : 0);
      const datePaths = effectiveDates
        ? expandDatePaths(localPath, effectiveDates)
        : [{ localPath, dateLabel: '' }];

      for (const dp of datePaths) {
        const dateTag = dp.dateLabel ? ` [${dp.dateLabel}]` : '';

        addLog('info', `[上传] ── 本地目录: ${dp.localPath} → 远程: ${remotePath}${dateTag}`);

        if (!fs.existsSync(dp.localPath)) {
          addLog('warn', `[上传]   本地目录不存在: ${dp.localPath}，跳过`);
          continue;
        }

        let localFiles;
        try {
          localFiles = fs.readdirSync(dp.localPath)
            .filter(name => {
              const full = path.join(dp.localPath, name);
              return fs.statSync(full).isFile() && regex.test(name);
            });
        } catch (e) {
          addLog('error', `[上传]   读取本地目录失败: ${e.message}`);
          continue;
        }

        addLog('info', `[上传]   本地匹配文件: ${localFiles.length} 个`);
        if (localFiles.length === 0) { addLog('info', '[上传]   没有匹配文件，跳过'); continue; }

        try {
          await client.ensureDir(remotePath);
          await client.cd(remotePath);
        } catch (e) {
          addLog('error', `[上传]   切换到远程目录失败: ${e.message}`);
          continue;
        }

        let uploaded = 0, skipped = 0, failed = 0;

        for (const name of localFiles) {
          const localFilePath = path.join(dp.localPath, name);

          if (skipUploaded && isAlreadyUploaded(localFilePath)) {
            addLog('info', `[上传]   跳过（已上传）: ${name}`);
            skipped++;
            continue;
          }

          try {
            const stat = fs.statSync(localFilePath);
            addLog('info', `[上传]   上传: ${name} (${formatSize(stat.size)})`);
            await retryWithBackoff(() => client.uploadFrom(localFilePath, name));
            markUploaded(localFilePath);
            uploaded++;
            addLog('info', `[上传]   ✓ 完成: ${name}`);
            if (deleteAfterUpload) {
              fs.unlinkSync(localFilePath);
              addLog('info', `[上传]   ✓ 已删除本地文件: ${name}`);
            }
          } catch (e) {
            failed++;
            addLog('error', `[上传]   ✗ 失败: ${name} - ${e.message}`);
          }
        }

        addLog('info', `[上传]   目录完成: 上传 ${uploaded}, 跳过 ${skipped}, 失败 ${failed}`);
        totalUploaded += uploaded;
        totalSkipped += skipped;
        totalFailed += failed;
      }
    }
  }

  const summary = `[上传] 本次结束 ── 上传: ${totalUploaded}, 跳过: ${totalSkipped}, 失败: ${totalFailed}`;
  addLog('info', summary);
  return { success: true, uploaded: totalUploaded, skipped: totalSkipped, failed: totalFailed, message: summary };
}


// ========== 文件监控 + 邮件通知 ==========

let monitorSnapshots = {};   // { monitorId: { filename: size } }
let mailTransporter = null;

function loadMonitorSnapshots() {
  try {
    if (fs.existsSync(MONITOR_SNAPSHOT_FILE)) {
      monitorSnapshots = JSON.parse(fs.readFileSync(MONITOR_SNAPSHOT_FILE, 'utf-8'));
    }
  } catch (e) {
    monitorSnapshots = {};
  }
}

function saveMonitorSnapshots() {
  try {
    fs.writeFileSync(MONITOR_SNAPSHOT_FILE, JSON.stringify(monitorSnapshots, null, 2), 'utf-8');
  } catch (_) {}
}

/** 取得邮件发送器；配置不完整时抛出具体错误 */
function getMailTransporter() {
  const m = config.mail || {};
  if (!m.host || !m.user || !m.password) {
    throw new Error('SMTP 未配置完整（host / 账号 / 授权码 都必须填写）');
  }
  if (!mailTransporter) {
    mailTransporter = nodemailer.createTransport({
      host: m.host,
      port: m.port || 465,
      secure: m.secure !== false,
      auth: { user: m.user, pass: m.password }
    });
  }
  return mailTransporter;
}

/** 发送通知邮件；失败直接抛异常（含原因） */
async function sendNotifyMail(subject, text) {
  const m = config.mail || {};
  const transporter = getMailTransporter();
  const to = m.to || m.user;
  if (!to) throw new Error('收件人邮箱未配置（mail.to 必填）');
  await transporter.sendMail({
    from: `"FTP文件监控" <${m.user}>`,
    to,
    subject,
    text
  });
  addLog('info', `监控通知邮件已发送至 ${to}：${subject}`);
}

/** 列出监控目录下的文件（SFTP / FTP 通用） */
async function listRemoteFilesForMonitor(remotePath, filePattern) {
  const protocol = config.ftp.protocol || 'sftp';
  const regex = patternToRegex(filePattern || '*');
  if (protocol === 'sftp') {
    const sftp = await ensureSftpConnected();
    const list = await sftp.list(remotePath || '/');
    return list
      .filter(item => item.type === '-' && regex.test(item.name))
      .map(f => ({ name: f.name, size: f.size }));
  }
  const client = await ensureFtpConnected();
  await client.cd(remotePath || '/');
  const list = await client.list();
  return list
    .filter(item => item.type === ftpLib.FileType.File && regex.test(item.name))
    .map(f => ({ name: f.name, size: f.size }));
}

/** 执行一次监控扫描：对比快照，新增文件即发邮件 */
async function runMonitor() {
  const enabledMonitors = (config.monitors || []).filter(m => m.enabled && m.remotePath);
  if (enabledMonitors.length === 0) {
    addLog('info', '[监控] 没有启用的监控项，跳过本次扫描');
    return { success: true, newFiles: 0 };
  }

  let totalNew = 0;
  for (const mon of enabledMonitors) {
    const id = mon.id;
    try {
      const files = await listRemoteFilesForMonitor(mon.remotePath, mon.filePattern);
      const prev = monitorSnapshots[id] || {};
      const current = {};
      const newFiles = [];
      for (const f of files) {
        current[f.name] = f.size;
        // 文件不存在 or 大小变化 → 视为新增
        if (!(f.name in prev) || prev[f.name] !== f.size) {
          newFiles.push(f);
        }
      }
      if (newFiles.length > 0) {
        const lines = newFiles.map(f => `  • ${f.name} (${formatSize(f.size)})`).join('\n');
        const subject = `[FTP监控] 新文件到达：${mon.remotePath}（${newFiles.length} 个）`;
        const text =
          `监控目录：${mon.remotePath}\n` +
          `检测到 ${newFiles.length} 个新文件：\n${lines}\n\n` +
          `时间：${new Date().toLocaleString('zh-CN')}`;
        try {
          await sendNotifyMail(subject, text);
        } catch (e) {
          addLog('error', `[监控] 发送通知失败：${e.message}`);
        }
        totalNew += newFiles.length;
      } else {
        addLog('info', `[监控] ${mon.remotePath} 无新增（当前 ${files.length} 个文件）`);
      }
      monitorSnapshots[id] = current;
    } catch (e) {
      addLog('error', `[监控] 检查 ${mon.remotePath} 失败: ${e.message}`);
    }
  }
  saveMonitorSnapshots();
  state.lastMonitorRun = new Date().toLocaleString('zh-CN');
  addLog('info', `[监控] 本次扫描结束，新增文件 ${totalNew} 个`);
  return { success: true, newFiles: totalNew };
}

function startMonitorScheduler() {
  stopMonitorScheduler();
  const mins = parseFloat(config.monitorInterval) || 5;
  if (mins < 1) {
    const ms = Math.round(mins * 60000);
    state.monitorRunning = true;
    runMonitor();
    state.monitorIntervalTimer = setInterval(() => { if (state.monitorRunning) runMonitor(); }, ms);
    addLog('info', `监控调度已启动，频率: 每 ${Math.round(ms / 1000)} 秒`);
  } else {
    const expr = mins === 1 ? '* * * * *' : `*/${Math.round(mins)} * * * *`;
    state.monitorRunning = true;
    state.monitorCronJob = cron.schedule(expr, () => { if (state.monitorRunning) runMonitor(); });
    addLog('info', `监控调度已启动，调度: ${expr}`);
  }
}

function stopMonitorScheduler() {
  if (state.monitorCronJob) { state.monitorCronJob.stop(); state.monitorCronJob = null; }
  if (state.monitorIntervalTimer) { clearInterval(state.monitorIntervalTimer); state.monitorIntervalTimer = null; }
  state.monitorRunning = false;
}

// ========== 归档报文对账 + 自动重传 ==========

let reuploadRecords = {};

function loadReuploadRecords() {
  try {
    if (fs.existsSync(REUPLOAD_RECORDS_FILE)) {
      reuploadRecords = JSON.parse(fs.readFileSync(REUPLOAD_RECORDS_FILE, 'utf-8'));
    }
  } catch (_) { reuploadRecords = {}; }
}

function saveReuploadRecords() {
  try { fs.writeFileSync(REUPLOAD_RECORDS_FILE, JSON.stringify(reuploadRecords, null, 2), 'utf-8'); } catch (_) {}
}

// 已验证一致的报文缓存：relPath -> "size|mtimeMs"，用于增量对账（跳过已验一致文件）
let verifiedCache = {};

function loadVerifiedCache() {
  try {
    if (fs.existsSync(RECONCILE_VERIFIED_FILE)) {
      verifiedCache = JSON.parse(fs.readFileSync(RECONCILE_VERIFIED_FILE, 'utf-8')) || {};
    }
  } catch (_) { verifiedCache = {}; }
}

function saveVerifiedCache() {
  try { fs.writeFileSync(RECONCILE_VERIFIED_FILE, JSON.stringify(verifiedCache, null, 2), 'utf-8'); } catch (_) {}
}

function clearVerifiedCache() {
  verifiedCache = {};
  try { if (fs.existsSync(RECONCILE_VERIFIED_FILE)) fs.unlinkSync(RECONCILE_VERIFIED_FILE); } catch (_) {}
}

/** 解析归档报文文件：文件内每一行都是一条独立记录（船名;航次;提单号;时间戳[;换单方式]），逐行返回数组 */
function parseManifestFile(fullPath, subDir) {
  try {
    const content = fs.readFileSync(fullPath, 'utf-8');
    // 兼容 Windows 生成的 \r\n 与 Linux 的 \n：按行切分并去除可能的 \r
    const lines = content.split(/\r?\n/).map(l => l.replace(/\r$/, '').trim()).filter(l => l);
    const records = [];
    for (const line of lines) {
      const parts = line.split(';').map(s => s.replace(/\r$/, '').trim());
      const [ship, voyage, bl, timestamp, releaseType] = parts;
      if (!ship || !voyage || !bl) continue;   // 该行字段不全，跳过
      records.push({
        ship, voyage, bl,
        timestamp: timestamp || '',
        releaseType: (releaseType !== undefined ? releaseType : ''),
        subDir,
        line
      });
    }
    return records.length ? records : null;
  } catch (_) { return null; }
}

/** 报文换单方式归一化：空/电放/21 → 电放；SWB/6 → SWB */
function normReleaseType(v) {
  if (v === undefined || v === null || v === '') return '电放';
  const s = String(v).trim().toUpperCase();
  if (s === '电放' || s === '21' || s === 'TELEX') return '电放';
  if (s === 'SWB' || s === '6') return 'SWB';
  return s;
}

/** DB SIGN_BL_TYPE 归一化：2/21→电放；6→SWB（注：实际库里是 2，非 21） */
function normDbSignBlType(v) {
  if (v === 2 || v === '2' || v === 21 || v === '21') return '电放';
  if (v === 6 || v === '6') return 'SWB';
  return String(v);
}

async function getOracleConnection() {
  const o = config.oracle || {};
  if (!o.host || !o.user || !o.password || !o.serviceName) {
    throw new Error('Oracle 未配置完整（host / user / password / serviceName 必填）');
  }
  return oracledb.getConnection({
    user: o.user,
    password: o.password,
    connectString: `${o.host}:${o.port || 1521}/${o.serviceName}`
  });
}

/** 按 船名/航次/提单号 查询放单状态；无记录返回 null */
async function queryManifest(conn, ship, voyage, bl) {
  const sql = `
    SELECT csm.SIGN_BL_INFO, csm.SIGN_BL_TYPE
    FROM CA_SI_MANIFEST csm
    LEFT JOIN CA_BM_SAILING_SCHEDULE cbss ON csm.VOYAGE_ID = cbss.VOYAGE_ID
    LEFT JOIN CA_BM_SHIP_CANONICAL cbsc ON cbss.SHIP_ID = cbsc.SHIP_ID
    WHERE csm.BL_NO = :bl
      AND cbsc.SHIP_EN_NAME = :ship
      AND cbss.IMP_VOYAGE_CODE = :voyage`;
  const r = await conn.execute(sql, { bl, ship, voyage });
  if (!r.rows || r.rows.length === 0) return null;
  const row = r.rows[0];
  return { signBlInfo: row[0], signBlType: row[1] };
}

/**
 * 批量查询一个文件内所有提单号的放单状态（一次 DB 往返）。
 * 返回 Map: key="船名|航次|提单号" -> { signBlInfo, signBlType }
 */
async function queryManifestBatch(conn, records) {
  const bls = [...new Set(records.map(r => r.bl).filter(Boolean))];
  const map = new Map();
  if (!bls.length) return map;
  const placeholders = bls.map((_, i) => `:bl${i}`).join(', ');
  const binds = {};
  bls.forEach((bl, i) => { binds[`bl${i}`] = bl; });
  const sql = `
    SELECT csm.BL_NO, cbsc.SHIP_EN_NAME, cbss.IMP_VOYAGE_CODE, csm.SIGN_BL_INFO, csm.SIGN_BL_TYPE
    FROM CA_SI_MANIFEST csm
    LEFT JOIN CA_BM_SAILING_SCHEDULE cbss ON csm.VOYAGE_ID = cbss.VOYAGE_ID
    LEFT JOIN CA_BM_SHIP_CANONICAL cbsc ON cbss.SHIP_ID = cbsc.SHIP_ID
    WHERE csm.BL_NO IN (${placeholders})`;
  const r = await conn.execute(sql, binds);
  for (const row of r.rows || []) {
    const key = `${row[1]}|${row[2]}|${row[0]}`;
    map.set(key, { signBlInfo: row[3], signBlType: row[4] });
  }
  return map;
}

/** 单条报文与 DB 比对，返回 { consistent, reason, localStatus, dbStatus } */
function compareOne(subDir, parsed, dbRow) {
  if (subDir === 'TelexRelease') {
    const fileNorm = normReleaseType(parsed.releaseType);
    if (!dbRow) return { consistent: false, reason: '数据库无记录', localStatus: fileNorm, dbStatus: '未查到' };
    const dbNorm = normDbSignBlType(dbRow.signBlType);
    const consistent = fileNorm === dbNorm;
    return {
      consistent,
      reason: consistent ? '' : `换单方式不一致(报文:${fileNorm}, 库:${dbNorm})`,
      localStatus: fileNorm,
      dbStatus: dbNorm
    };
  }
  // FreightConfirm：报文隐含已放单；DB 查不到 或 状态为不可放(0) → 不一致
  if (!dbRow) return { consistent: false, reason: '数据库无记录', localStatus: '已放单', dbStatus: '未查到' };
  const info = parseInt(dbRow.signBlInfo);
  if (info === 0) return { consistent: false, reason: '数据库状态为不可放(0)', localStatus: '已放单', dbStatus: '不可放' };
  return { consistent: true, reason: '', localStatus: '已放单', dbStatus: '可放' };
}

/** 把不一致报文重传到对应远程目录（文件名不变） */
async function reuploadMismatch(item) {
  const remoteSub = '/' + item.subDir; // /FreightConfirm | /TelexRelease
  const fname = path.basename(item.localPath);
  try {
    const proto = config.ftp.protocol || 'sftp';
    if (proto === 'sftp') {
      const sftp = await ensureSftpConnected();
      const remotePath = remoteSub.replace(/\/+$/, '') + '/' + fname;
      await retryWithBackoff(() => sftp.fastPut(item.localPath, remotePath));
    } else {
      const client = await ensureFtpConnected();
      await client.cd(remoteSub);
      await retryWithBackoff(() => client.uploadFrom(item.localPath, fname));
    }
    const existing = reuploadRecords[item.localPath] || {};
    reuploadRecords[item.localPath] = Object.assign({}, existing, {
      size: fs.statSync(item.localPath).size,
      reuploadedAt: Date.now(),
      subDir: item.subDir,
      localStatus: item.localStatus,
      dbStatus: item.dbStatus,
      reason: item.reason,
      result: 'success'
    });
    saveReuploadRecords();
    addLog('info', `[重传] ✓ ${fname} → ${remoteSub}`);
    return { success: true };
  } catch (e) {
    reuploadRecords[item.localPath] = Object.assign({}, reuploadRecords[item.localPath], {
      reuploadedAt: Date.now(),
      subDir: item.subDir,
      localStatus: item.localStatus,
      dbStatus: item.dbStatus,
      reason: item.reason,
      result: 'failed',
      error: e.message
    });
    saveReuploadRecords();
    addLog('error', `[重传] ✗ ${fname}: ${e.message}`);
    return { success: false, error: e.message };
  }
}

/** 扫描归档 + 查 DB + 比对；autoReupload 时自动重传。已验证一致的文件增量跳过。 */
async function runReconcile() {
  // 自管理运行标志，避免串行调用互相阻塞 / 手动触发永远 busy
  if (state.reconcileRunning) {
    return { success: false, busy: true, message: '⏳ 对账正在执行中，请稍候...', scanned: 0, matched: 0, mismatched: 0, skipped: 0, mismatches: [] };
  }
  state.reconcileRunning = true;
  let result;
  try {
    result = await _doReconcile();
  } finally {
    state.reconcileRunning = false;
  }
  return result;
}

async function _doReconcile() {
  if (!config.oracle || !config.oracle.enabled) {
    return { success: false, message: 'Oracle 未启用', scanned: 0, matched: 0, mismatched: 0, skipped: 0, mismatches: [] };
  }
  const windowDays = parseInt(config.reconcile.windowDays) || 7;
  const baseDir = config.localDir;
  if (!baseDir || !fs.existsSync(baseDir)) {
    return { success: false, message: '本地归档目录未配置或不存在', scanned: 0, matched: 0, mismatched: 0, skipped: 0, mismatches: [] };
  }
  const subDirs = ['FreightConfirm', 'TelexRelease'];
  const today = new Date();
  let scanned = 0, matched = 0, mismatched = 0, skipped = 0;
  const mismatches = [];
  const seenThisRun = new Set();   // 本次扫描到的文件 relPath，用于清理已删除文件的缓存
  let conn;
  try {
    conn = await getOracleConnection();
    for (let d = 0; d < windowDays; d++) {
      const date = new Date(today);
      date.setDate(date.getDate() - d);
      const dateStr = date.getFullYear() + '-' +
        String(date.getMonth() + 1).padStart(2, '0') + '-' +
        String(date.getDate()).padStart(2, '0');
      for (const sub of subDirs) {
        const dir = path.join(baseDir, dateStr, sub);
        if (!fs.existsSync(dir)) continue;
        let files = [];
        try {
          files = fs.readdirSync(dir).filter(f => {
            try { return fs.statSync(path.join(dir, f)).isFile(); } catch (_) { return false; }
          });
        } catch (_) { continue; }
        for (const fname of files) {
          const full = path.join(dir, fname);
          let st;
          try { st = fs.statSync(full); } catch (_) { continue; }
          const rel = path.relative(baseDir, full);
          seenThisRun.add(rel);
          // 增量：签名（大小+修改时间）与缓存一致 → 上次已验一致，跳过
          const sig = st.size + '|' + st.mtimeMs;
          if (verifiedCache[rel] === sig) { skipped++; continue; }

          const records = parseManifestFile(full, sub);   // 数组：文件内逐行记录
          if (!records || !records.length) continue;
          scanned += records.length;
          // 一次批量查出该文件所有提单号的 DB 状态
          let batchMap;
          try {
            batchMap = await queryManifestBatch(conn, records);
          } catch (e) {
            addLog('error', `[对账] 批量查询失败 ${fname}: ${e.message}`);
            continue;
          }
          let fileMismatch = null;   // 文件级：任一行不一致即标记重传
          for (const parsed of records) {
            const dbRow = batchMap.get(`${parsed.ship}|${parsed.voyage}|${parsed.bl}`) || null;
            const cmp = compareOne(sub, parsed, dbRow);
            if (!cmp.consistent) {
              if (!fileMismatch) fileMismatch = Object.assign({ file: fname, localPath: full, subDir: sub }, parsed, cmp);
              mismatched++;
            } else {
              matched++;
            }
          }
          if (fileMismatch) {
            mismatches.push(fileMismatch);   // 整文件只记一条（重传去重）
          } else {
            verifiedCache[rel] = sig;        // 全部一致 → 记入缓存，下次跳过
          }
        }
      }
    }

    // 清理缓存中已不存在的文件，避免无限增长
    for (const k of Object.keys(verifiedCache)) {
      if (!seenThisRun.has(k)) {
        const p = path.join(baseDir, k);
        if (!fs.existsSync(p)) delete verifiedCache[k];
      }
    }
    saveVerifiedCache();
  } finally {
    if (conn) { try { await conn.close(); } catch (_) {} }
  }

  state.lastReconcileRun = new Date().toLocaleString('zh-CN');
  state.lastReconcileSkipped = skipped;
  addLog('info', `[对账] 完成：本次验证 ${scanned} 条，跳过已验一致 ${skipped} 个文件，一致 ${matched}，不一致 ${mismatched}`);

  // 无论是否自动重传，都先更新"最后检查时间"，让记录时间戳保持新鲜
  const reuploadSeen = new Set();
  for (const m of mismatches) {
    if (reuploadSeen.has(m.localPath)) continue;
    reuploadSeen.add(m.localPath);
    const existing = reuploadRecords[m.localPath] || {};
    reuploadRecords[m.localPath] = Object.assign({}, existing, {
      lastCheckedAt: Date.now(),
      subDir: m.subDir,
      localStatus: m.localStatus,
      dbStatus: m.dbStatus,
      reason: m.reason,
      // result / reuploadedAt 不动，等实际重传时再更新
    });
  }
  saveReuploadRecords();

  if (config.reconcile.autoReupload) {
    for (const m of mismatches) {
      if (!reuploadSeen.has(m.localPath)) continue;   // 同一文件只重传一次（即使多行不一致）
      await reuploadMismatch(m);
    }
  }
  return { success: true, scanned, matched, mismatched, skipped, mismatches };
}

function startReconcileScheduler() {
  stopReconcileScheduler();
  if (!config.reconcile || !config.reconcile.enabled) return;
  const mins = parseFloat(config.reconcile.interval) || 30;
  if (mins < 1) {
    const ms = Math.round(mins * 60000);
    runReconcile();   // 立即跑一次（自带运行标志防重入）
    state.reconcileIntervalTimer = setInterval(() => { runReconcile(); }, ms);
    addLog('info', `对账调度已启动，频率: 每 ${Math.round(ms / 1000)} 秒`);
  } else {
    const expr = mins === 1 ? '* * * * *' : `*/${Math.round(mins)} * * * *`;
    state.reconcileCronJob = cron.schedule(expr, () => { runReconcile(); });
    addLog('info', `对账调度已启动，调度: ${expr}`);
  }
}

function stopReconcileScheduler() {
  if (state.reconcileCronJob) { state.reconcileCronJob.stop(); state.reconcileCronJob = null; }
  if (state.reconcileIntervalTimer) { clearInterval(state.reconcileIntervalTimer); state.reconcileIntervalTimer = null; }
  state.reconcileRunning = false;
}


// ========== 调度器 ==========

function calcNextRun() {
  if (!state.running) return null;
  const { type, interval } = config.schedule;
  if (type === 'interval') {
    const ms = (parseInt(interval) || 10) * 60 * 1000;
    return new Date(Date.now() + ms).toLocaleString('zh-CN');
  }
  return '按Cron计划执行';
}

/** 构建调度 tick 函数（可传入要执行的任务列表） */
function makeTick(tasks) {
  return async () => {
    if (!state.running) return;
    addLog('info', '══════ 定时任务触发 ══════');
    for (const t of tasks) await t();
  };
}

/** 根据调度配置启动一个定时器（返回 { cronJob, intervalTimer }） */
function scheduleTask(schedConf, tickFn, label) {
  const { type, interval, cronExpr } = schedConf;
  if (type === 'interval') {
    const mins = parseFloat(interval) || 10;
    if (mins < 1) {
      const ms = Math.round(mins * 60000);
      addLog('info', `${label} 已启动，频率: 每 ${(ms/1000).toFixed(0)} 秒`);
      tickFn(); // 启动时立即执行一次
      const timer = setInterval(tickFn, ms);
      return { cronJob: null, intervalTimer: timer };
    }
    const expr = mins === 1 ? '* * * * *' : `*/${Math.round(mins)} * * * *`;
    addLog('info', `${label} 已启动，调度: ${expr}`);
    const job = cron.schedule(expr, tickFn);
    return { cronJob: job, intervalTimer: null };
  }
  const expr = cronExpr || '*/10 * * * *';
  if (!cron.validate(expr)) {
    addLog('error', `${label} 无效的Cron表达式: ${expr}`);
    return null;
  }
  addLog('info', `${label} 已启动，调度: ${expr}`);
  const job = cron.schedule(expr, tickFn);
  return { cronJob: job, intervalTimer: null };
}

function startScheduler() {
  stopScheduler();
  state.running = true;

  // 下载任务的 tick
  const downloadTick = async () => {
    await fetchFiles();
    state.nextRun = calcNextRun();
  };

  // 判断上传是否与下载同步还是独立
  const uploadEnabled = config.uploadEnabled && (config.uploadFolders || []).some(f => f.enabled);
  const uploadSameAsDownload = !config.uploadSchedule || config.uploadSchedule.type === 'same';

  // 下载（+ 可选同步上传）的 tick
  const mainTick = async () => {
    if (!state.running) return;
    addLog('info', '══════ 定时任务触发 ══════');
    await fetchFiles();
    state.nextRun = calcNextRun();
    if (uploadEnabled && uploadSameAsDownload) {
      await uploadFiles();
    }
  };

  // 启动下载调度
  const dlResult = scheduleTask(config.schedule, mainTick, '下载任务');
  if (!dlResult) { state.running = false; return false; }
  state.cronJob = dlResult.cronJob;
  state.intervalTimer = dlResult.intervalTimer;
  state.nextRun = calcNextRun();

  // 启动上传独立调度（仅当 uploadSchedule.type !== 'same'）
  if (uploadEnabled && !uploadSameAsDownload) {
    const uploadTick = async () => {
      if (!state.running) return;
      addLog('info', '══════ 上传任务触发 ══════');
      await uploadFiles();
    };
    const ulResult = scheduleTask(config.uploadSchedule, uploadTick, '上传任务');
    if (ulResult) {
      state.uploadCronJob = ulResult.cronJob;
      state.uploadIntervalTimer = ulResult.intervalTimer;
    }
  }

  return true;
}

function stopScheduler() {
  if (state.cronJob) { state.cronJob.stop(); state.cronJob = null; }
  if (state.intervalTimer) { clearInterval(state.intervalTimer); state.intervalTimer = null; }
  if (state.uploadCronJob) { state.uploadCronJob.stop(); state.uploadCronJob = null; }
  if (state.uploadIntervalTimer) { clearInterval(state.uploadIntervalTimer); state.uploadIntervalTimer = null; }
  state.running = false;
  state.nextRun = null;
}

// ========== API ==========

app.get('/api/config', (req, res) => res.json(config));

app.post('/api/config', (req, res) => {
  try {
    const oldFp = getConnFingerprint();
    const wasRunning = state.running;
    config = deepMerge(config, req.body);
    saveConfig();
    // 若 FTP 连接信息变更，断开旧连接
    if (getConnFingerprint() !== oldFp) {
      disconnectAll();
      addLog('info', 'FTP 连接信息已变更，持久连接已断开，将在下次抓取时自动重建');
    }
    // 若调度器正在运行，用新配置重启以生效
    if (wasRunning) {
      startScheduler();
      addLog('info', '配置已保存并已自动重启调度任务');
    } else {
      addLog('info', '配置已保存');
    }
    // 对账调度：按新配置重启（无论之前是否运行）
    stopReconcileScheduler();
    if (config.reconcile && config.reconcile.enabled && config.oracle && config.oracle.enabled) {
      startReconcileScheduler();
      addLog('info', '配置已保存并已自动重启对账调度');
    }
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.get('/api/status', (req, res) => res.json({
  running: state.running,
  lastRun: state.lastRun,
  lastStatus: state.lastStatus,
  nextRun: state.nextRun,
  logCount: state.logs.length
}));

app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 200;
  res.json(state.logs.slice(0, limit));
});

app.delete('/api/logs', (req, res) => {
  state.logs = [];
  addLog('info', '日志已清空');
  res.json({ success: true });
});

app.post('/api/start', (req, res) => {
  const ok = startScheduler();
  res.json({ success: ok, running: state.running });
});

app.post('/api/stop', (req, res) => {
  stopScheduler();
  addLog('info', '定时任务已手动停止');
  res.json({ success: true, running: false });
});

app.post('/api/fetch-now', async (req, res) => {
  addLog('info', '══════ 手动触发立即抓取 ══════');
  const result = await fetchFiles();
  res.json(result);
});

// 手动立即上传
app.post('/api/upload-now', async (req, res) => {
  addLog('info', '══════ 手动触发立即上传 ══════');
  const result = await uploadFiles();
  res.json(result);
});

// 获取已上传记录统计
app.get('/api/upload-records', (req, res) => {
  const keys = Object.keys(uploadRecords);
  res.json({ total: keys.length, records: keys.slice(0, 100) });
});

// 清除上传记录（允许重新上传所有文件）
app.delete('/api/upload-records', (req, res) => {
  uploadRecords = {};
  saveUploadRecords();
  addLog('info', '已清空上传记录，所有文件将在下次上传时重新处理');
  res.json({ success: true });
});

// 测试连接
app.post('/api/test-connection', async (req, res) => {
  const testConf = req.body || config.ftp;
  // 兜底：如果端口是22，强制走 SFTP；只有端口21才走 FTP
  let protocol = testConf.protocol;
  if (!protocol) {
    protocol = (parseInt(testConf.port) === 21) ? 'ftp' : 'sftp';
  }
  // 额外保险：如果前端发来的 protocol 是 ftp 但端口是 22，也走 sftp
  if (protocol === 'ftp' && parseInt(testConf.port) === 22) {
    protocol = 'sftp';
    addLog('warn', '检测到端口22但协议为FTP，已自动修正为SFTP');
  }
  console.log('[DEBUG] test-connection protocol:', protocol, 'port:', testConf.port, 'body:', JSON.stringify(testConf).substring(0, 100));

  if (protocol === 'sftp') {
    let sftp;
    try {
      sftp = await sftpConnect(testConf);
      const list = await sftp.list(testConf.testPath || '/');
      await sftp.end();
      addLog('info', `SFTP 连接测试成功，根目录有 ${list.length} 个条目`);
      res.json({ success: true, message: `SFTP 连接成功！根目录共 ${list.length} 个文件/文件夹` });
    } catch (e) {
      try { if (sftp) await sftp.end(); } catch (_) {}
      addLog('warn', `SFTP 连接测试失败: ${e.message}`);
      res.json({ success: false, message: e.message });
    }
  } else {
    const client = new ftpLib.Client();
    try {
      await client.access({
        host: testConf.host,
        port: testConf.port || 21,
        user: testConf.user || 'anonymous',
        password: testConf.password || '',
        secure: testConf.secure || false
      });
      const list = await client.list(testConf.testPath || '/');
      client.close();
      addLog('info', `FTP 连接测试成功，根目录有 ${list.length} 个条目`);
      res.json({ success: true, message: `FTP 连接成功！根目录共 ${list.length} 个文件/文件夹` });
    } catch (e) {
      try { client.close(); } catch (_) {}
      addLog('warn', `FTP 连接测试失败: ${e.message}`);
      res.json({ success: false, message: e.message });
    }
  }
});

// 浏览远程目录（复用持久连接）
app.post('/api/ftp-list', async (req, res) => {
  const { remotePath } = req.body;
  const ftpConf = config.ftp;
  const protocol = ftpConf.protocol || 'sftp';

  if (protocol === 'sftp') {
    let sftp;
    try {
      sftp = await ensureSftpConnected();
      const list = await sftp.list(remotePath || '/');
      const items = list.map(item => ({
        name: item.name,
        type: item.type === 'd' ? 'dir' : 'file',
        size: item.size,
        date: item.modifyTime
      }));
      res.json({ success: true, path: remotePath || '/', items });
    } catch (e) {
      res.json({ success: false, message: e.message });
    }
  } else {
    let client;
    try {
      client = await ensureFtpConnected();
      const list = await client.list(remotePath || '/');
      const items = list.map(item => ({
        name: item.name,
        type: item.type === ftpLib.FileType.Directory ? 'dir' : 'file',
        size: item.size,
        date: item.rawModifiedAt
      }));
      res.json({ success: true, path: remotePath || '/', items });
    } catch (e) {
      res.json({ success: false, message: e.message });
    }
  }
});

// 浏览本地目录
app.post('/api/browse-local', (req, res) => {
  const { dir } = req.body;
  try {
    const target = dir || require('os').homedir();
    const items = fs.readdirSync(target, { withFileTypes: true })
      .filter(d => d.isDirectory())
      .map(d => ({ name: d.name, path: path.join(target, d.name) }));
    const parent = path.dirname(target);
    res.json({ current: target, parent: parent !== target ? parent : null, items });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// 归档目录树
app.get('/api/archive-tree', (req, res) => {
  const baseDir = config.localDir;
  if (!baseDir || !fs.existsSync(baseDir)) return res.json({ tree: [] });

  function readTree(dir, depth) {
    if (depth > 2) return [];
    try {
      return fs.readdirSync(dir, { withFileTypes: true })
        .filter(d => d.isDirectory())
        .map(d => {
          const fullPath = path.join(dir, d.name);
          const files = fs.readdirSync(fullPath).filter(f => {
            try { return fs.statSync(path.join(fullPath, f)).isFile(); } catch (_) { return false; }
          });
          return { name: d.name, path: fullPath, fileCount: files.length, children: readTree(fullPath, depth + 1) };
        });
    } catch (_) { return []; }
  }

  res.json({ baseDir, tree: readTree(baseDir, 0) });
});

// ========== 文件监控 API ==========

// 监控状态
app.get('/api/monitor/status', (req, res) => res.json({
  running: state.monitorRunning,
  lastRun: state.lastMonitorRun,
  enabled: config.monitorEnabled,
  interval: config.monitorInterval,
  count: (config.monitors || []).length
}));

// 启动监控调度
app.post('/api/monitor/start', (req, res) => {
  startMonitorScheduler();
  res.json({ success: true, running: state.monitorRunning });
});

// 停止监控调度
app.post('/api/monitor/stop', (req, res) => {
  stopMonitorScheduler();
  res.json({ success: true, running: false });
});

// 立即扫描一次
app.post('/api/monitor/scan-now', async (req, res) => {
  const result = await runMonitor();
  res.json(result);
});

// 重置指定监控项的快照（下次扫描把所有当前文件视为新增）
app.delete('/api/monitor/snapshot/:id', (req, res) => {
  const id = req.params.id;
  delete monitorSnapshots[id];
  saveMonitorSnapshots();
  addLog('info', `已重置监控快照: ${id}`);
  res.json({ success: true });
});

// 发送测试邮件（允许临时指定收件人）
app.post('/api/test-mail', async (req, res) => {
  try {
    const { to } = req.body || {};
    const m = config.mail || {};
    if (to) m.to = to;   // 测试时临时指定收件人，不落盘
    await sendNotifyMail('[FTP监控] 测试邮件', '这是一封来自 FTP 文件监控工具的测试邮件，收到说明 SMTP 配置正确。');
    res.json({ success: true, message: '测试邮件已发送，请查收' });
  } catch (e) {
    res.json({ success: false, message: e.message });
  }
});

// ========== 归档报文对账 API ==========

// 测试 Oracle 连接
app.post('/api/oracle/test', async (req, res) => {
  try {
    const o = config.oracle || {};
    if (!o.host || !o.user || !o.password || !o.serviceName) {
      return res.json({ success: false, message: 'Oracle 未配置完整' });
    }
    const conn = await oracledb.getConnection({
      user: o.user, password: o.password,
      connectString: `${o.host}:${o.port || 1521}/${o.serviceName}`
    });
    await conn.execute('SELECT 1 AS ok FROM dual');
    await conn.close();
    res.json({ success: true, message: 'Oracle 连接成功' });
  } catch (e) {
    res.json({ success: false, message: e.message });
  }
});

// 对账状态
app.get('/api/reconcile/status', (req, res) => res.json({
  running: state.reconcileRunning,
  lastRun: state.lastReconcileRun,
  enabled: config.reconcile.enabled,
  interval: config.reconcile.interval,
  windowDays: config.reconcile.windowDays,
  autoReupload: config.reconcile.autoReupload,
  oracleEnabled: config.oracle.enabled,
  skipped: state.lastReconcileSkipped || 0,
  verifiedCacheSize: Object.keys(verifiedCache).length
}));

// 立即对账
app.post('/api/reconcile/run', async (req, res) => {
  const result = await runReconcile();
  res.json(result);
});

// 清空已验证缓存（下次全量重新对账）
app.post('/api/reconcile/clear-cache', (req, res) => {
  clearVerifiedCache();
  addLog('info', '[对账] 已清空已验证缓存，下次将全量重新核对');
  res.json({ success: true, message: '已清空已验证缓存，下次将全量重新核对' });
});

// 重传记录
app.get('/api/reupload-records', (req, res) => {
  const list = Object.entries(reuploadRecords).map(([k, v]) => ({ localPath: k, ...v }));
  list.sort((a, b) => (b.reuploadedAt || 0) - (a.reuploadedAt || 0));
  res.json({ records: list });
});

const PORT = parseInt(process.env.PORT, 10) || 3721;
loadConfig();
// 环境变量覆盖（无头 / 容器部署）：在读取 config.json 之后生效，优先级最高
applyEnvOverrides();
// 首次运行且无 config.json 时，用当前配置初始化一个文件，方便界面保存
ensureConfigFile();
loadUploadRecords();
loadMonitorSnapshots();
loadReuploadRecords();
loadVerifiedCache();

// 进程退出时释放所有连接
process.on('SIGINT', () => { disconnectAll(); process.exit(0); });
process.on('SIGTERM', () => { disconnectAll(); process.exit(0); });

// 全局错误捕获，防止进程崩溃
process.on('uncaughtException', (err) => {
  console.error('[FATAL] uncaughtException:', err.message);
  addLog('error', `进程异常: ${err.message}`);
});
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] unhandledRejection:', reason);
  addLog('error', `未处理的Promise异常: ${reason}`);
});

app.listen(PORT, () => {
  console.log(`\n🚀 FTP/SFTP 定时抓取工具已启动`);
  console.log(`📌 访问地址: http://localhost:${PORT}\n`);
  addLog('info', `服务启动，端口 ${PORT}`);

  // 启动后自动运行调度器（如果配置完整）
  if (config.ftp && config.ftp.host && config.folders && config.folders.some(f => f.enabled && f.remotePath)) {
    startScheduler();
    addLog('info', '已自动启动定时任务');
  }

  // 启动后自动恢复监控调度（若 monitorEnabled=true）
  if (config.monitorEnabled) {
    startMonitorScheduler();
    addLog('info', '已自动启动文件监控');
  }

  // 启动后自动恢复对账调度（若 reconcile.enabled=true 且 Oracle 已启用）
  if (config.reconcile && config.reconcile.enabled && config.oracle && config.oracle.enabled) {
    startReconcileScheduler();
    addLog('info', '已自动启动归档报文对账');
  }
});
