import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { spawn, execFile, ChildProcess } from 'child_process';

export type VideoEncoder = 'h264_v4l2m2m' | 'libx264';

@Injectable()
export class FfmpegStreamerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(FfmpegStreamerService.name);
  private processes = new Map<string, { process: ChildProcess; rtspUrl: string; backendIp: string; videoPort: number; audioPort: number }>();
  private retryCounts = new Map<string, number>();
  private readonly MAX_RETRIES = 1; // 1 reintento local rápido antes de notificar fallo definitivo al backend
  private readonly RETRY_DELAY_MS = 2000;
  public onStreamFailed?: (cameraId: string) => void;
  private activeEncoder: VideoEncoder = 'libx264';

  async onModuleInit() {
    await this.resolveVideoEncoder();
  }

  public getActiveEncoder(): VideoEncoder {
    return this.activeEncoder;
  }

  public isStreamActive(cameraId: string): boolean {
    return this.processes.has(cameraId);
  }

  /**
   * Resuelve el codificador de video disponible:
   * Prioriza h264_v4l2m2m (Hardware V4L2 M2M) si está disponible y validado por smoke-test.
   * En caso contrario realiza fallback transparente a libx264 (Software).
   */
  private async resolveVideoEncoder(): Promise<void> {
    const mode = (process.env.VIDEO_ENCODER_MODE || 'auto').toLowerCase();

    if (mode === 'sw') {
      this.activeEncoder = 'libx264';
      this.logger.log('Codificador forzado a software (libx264) vía VIDEO_ENCODER_MODE=sw.');
      return;
    }

    if (process.platform !== 'linux' && mode === 'auto') {
      this.activeEncoder = 'libx264';
      this.logger.log(`Plataforma ${process.platform} no es Linux (V4L2 M2M no aplica). Usando libx264.`);
      return;
    }

    const hwWorking = await this.testV4L2Encoder();
    if (hwWorking) {
      this.activeEncoder = 'h264_v4l2m2m';
      this.logger.log('✅ Aceleración por hardware V4L2 M2M detectada y activada: h264_v4l2m2m');
    } else {
      if (mode === 'hw') {
        this.logger.error('❌ Se configuró VIDEO_ENCODER_MODE=hw pero h264_v4l2m2m no respondió al test de hardware.');
      }
      this.activeEncoder = 'libx264';
      this.logger.log('ℹ️ h264_v4l2m2m no disponible en este dispositivo. Fallback automático a libx264 (CPU).');
    }
  }

  /**
   * Realiza una prueba sintética no bloqueante (dry run) para verificar si el
   * encoder h264_v4l2m2m y el dispositivo /dev/video* del kernel responden adecuadamente.
   */
  private testV4L2Encoder(): Promise<boolean> {
    return new Promise((resolve) => {
      const args = ['-f', 'lavfi', '-i', 'nullsrc=s=64x64:d=0.1', '-c:v', 'h264_v4l2m2m', '-f', 'null', '-'];
      this.logger.debug(`Ejecutando prueba sintética de encoder HW: ffmpeg ${args.join(' ')}`);

      let finished = false;
      const timeout = setTimeout(() => {
        if (!finished) {
          finished = true;
          try {
            testProcess.kill('SIGKILL');
          } catch (e) {
            // ignore
          }
          resolve(false);
        }
      }, 3000);

      const testProcess = spawn('ffmpeg', args);

      testProcess.on('close', (code) => {
        if (!finished) {
          finished = true;
          clearTimeout(timeout);
          resolve(code === 0);
        }
      });

      testProcess.on('error', () => {
        if (!finished) {
          finished = true;
          clearTimeout(timeout);
          resolve(false);
        }
      });
    });
  }

  /**
   * Construye los argumentos de video respetando los requerimientos de WebRTC
   * (Baseline profile, sin B-frames, GOP fijo de 30) y diferenciando las opciones
   * exclusivas de libx264 de las de h264_v4l2m2m.
   */
  private buildVideoArgs(): string[] {
    const baseArgs = [
      '-map', '0:v:0',
      '-vf', 'scale=-2:720,fps=15,setpts=PTS-STARTPTS', // fps=15 suaviza el jitter de red TCP usando el reloj local
      '-pix_fmt', 'yuv420p',
      '-g', '30', // Keyframe (I-frame) cada 30 frames para que WebRTC arranque rápido
      '-bf', '0', // Desactiva B-frames, crucial para WebRTC
    ];

    if (this.activeEncoder === 'h264_v4l2m2m') {
      return [
        ...baseArgs,
        '-c:v', 'h264_v4l2m2m',
        '-b:v', '1000k',
      ];
    }

    // Configuración para libx264 (Software)
    return [
      ...baseArgs,
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-tune', 'zerolatency',
      '-profile:v', 'baseline',
      '-level', '3.1',
      '-b:v', '1000k',
      '-minrate', '1000k', // FORZAR padding de bitrate cuando no hay movimiento
      '-maxrate', '1000k',
      '-bufsize', '2000k',
      '-x264opts', 'nal-hrd=cbr', // Rellenar con NAL units vacíos para mantener la red activa
      '-sc_threshold', '0', // Evitar keyframes dinámicos que rompen el GOP en WebRTC
    ];
  }

  // Verifica si el flujo RTSP contiene una pista de audio
  private async hasAudio(rtspUrl: string): Promise<boolean> {
    return new Promise((resolve) => {
      const args = [
        '-rtsp_transport', 'tcp',
        '-v', 'error',
        '-select_streams', 'a',
        '-show_entries', 'stream=codec_type',
        '-of', 'default=nw=1:nk=1',
        rtspUrl,
      ];
      this.logger.log(`Probing RTSP audio streams (TCP): ffprobe ${args.join(' ')}`);

      execFile('ffprobe', args, { timeout: 8000 }, (err, stdout) => {
        if (err) {
          this.logger.warn(`FFprobe para audio falló o superó el tiempo límite. Asumiendo sin audio: ${err.message}`);
          resolve(false);
          return;
        }
        const result = stdout.toString().trim();
        this.logger.log(`Resultado del análisis de audio FFprobe: "${result}"`);
        resolve(result === 'audio');
      });
    });
  }

  // Verifica el códec de video del flujo RTSP
  private async getVideoCodec(rtspUrl: string): Promise<string> {
    return new Promise((resolve) => {
      const args = [
        '-rtsp_transport', 'tcp',
        '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name',
        '-of', 'default=nw=1:nk=1',
        rtspUrl,
      ];
      this.logger.log(`Probing RTSP video codec (TCP): ffprobe ${args.join(' ')}`);

      execFile('ffprobe', args, { timeout: 8000 }, (err, stdout) => {
        if (err) {
          this.logger.warn(`FFprobe para video falló o superó el tiempo límite. Asumiendo h264: ${err.message}`);
          resolve('h264');
          return;
        }
        const result = stdout.toString().trim();
        this.logger.log(`Resultado del análisis de video FFprobe: "${result}"`);
        resolve(result || 'h264');
      });
    });
  }

  async startStreaming(cameraId: string, rtspUrl: string, backendIp: string, videoPort: number, audioPort: number): Promise<boolean> {
    if (this.processes.has(cameraId)) {
      this.logger.warn(`El stream de la cámara ${cameraId} ya está activo. Deteniéndolo antes de reiniciar.`);
      this.stopStreaming(cameraId);
    }

    this.logger.log(`Analizando flujo RTSP de la cámara ${cameraId} (${rtspUrl})...`);
    const [streamHasAudio, videoCodec] = await Promise.all([
      this.hasAudio(rtspUrl),
      this.getVideoCodec(rtspUrl)
    ]);

    this.logger.log(
      `Iniciando FFmpeg para cámara ${cameraId}. Video codec origen: ${videoCodec}, Encoder destino: [${this.activeEncoder}], Audio detectado: ${streamHasAudio}. Destino: ${backendIp} (Video: ${videoPort}, Audio: ${audioPort})`
    );

    // Construir argumentos dinámicamente según encoder activo y presencia de audio con máxima tolerancia a fallos
    const args = [
      '-rtsp_transport', 'tcp',
      '-rtsp_flags', 'prefer_tcp',
      '-buffer_size', '4096000', // Buffer de 4MB para absorber ráfagas de I-frames y evitar CSeq desync
      '-stimeout', '10000000', // 10s timeout de socket RTSP en microsegundos
      '-timeout', '10000000', // 10s timeout de lectura/conexión
      '-reorder_queue_size', '4000', // Tolerar paquetes desordenados por jitter de red local
      '-max_delay', '500000', // Tolerar hasta 500ms de jitter de red
      '-err_detect', 'ignore_err', // Ignorar errores de NAL y slices dañados en vez de cerrar
      '-fflags', '+genpts+discardcorrupt+nobuffer', // Descartar paquetes corruptos sin frenar el flujo
      '-use_wallclock_as_timestamps', '1', // Obliga a FFmpeg a procesar los paquetes en tiempo real sin esperar al siguiente
      '-user_agent', 'VLC/3.0.16 LibVLC/3.0.16',
      '-i', rtspUrl,
      ...this.buildVideoArgs(),
      '-f', 'rtp',
      '-ssrc', '11111',
      '-payload_type', '101',
      `rtp://${backendIp}:${videoPort}?pkt_size=1200`
    ];

    if (streamHasAudio) {
      args.push(
        '-map', '0:a:0',
        '-c:a', 'libopus',
        '-ab', '128k',
        '-ar', '48000',
        '-ac', '2',
        '-af', 'aresample=async=1,asetpts=PTS-STARTPTS', // Sincronizar audio y reescribir timestamps desde 0
        '-f', 'rtp',
        '-ssrc', '22222',
        '-payload_type', '102',
        `rtp://${backendIp}:${audioPort}?pkt_size=1200`
      );
    }

    args.push('-y');

    this.logger.log(`Spawning FFmpeg con argumentos: ffmpeg ${args.join(' ')}`);
    const ffmpegProcess = spawn('ffmpeg', args);

    ffmpegProcess.stdout.on('data', (data) => {
      this.logger.debug(`[FFmpeg-cam-${cameraId}-stdout]: ${data}`);
    });

    ffmpegProcess.stderr.on('data', (data) => {
      const msg = data.toString();
      // Filtrar estadísticas ruidosas
      if (!msg.includes('frame=') && !msg.includes('speed=')) {
        this.logger.warn(`[FFmpeg-cam-${cameraId}]: ${msg.trim()}`);
      }
    });

    ffmpegProcess.on('close', (code) => {
      this.logger.warn(`El proceso FFmpeg de la cámara ${cameraId} se cerró con código: ${code}`);

      const processInfo = this.processes.get(cameraId);
      this.processes.delete(cameraId);

      if (processInfo) {
        // Cierre no intencional (stopStreaming no fue llamado). Intentar recuperación.
        this.handleRestart(cameraId, processInfo);
      }
    });

    ffmpegProcess.on('error', (err) => {
      this.logger.error(`Error en el proceso FFmpeg de la cámara ${cameraId}:`, err);
    });

    this.processes.set(cameraId, {
      process: ffmpegProcess,
      rtspUrl,
      backendIp,
      videoPort,
      audioPort
    });

    this.retryCounts.set(cameraId, 0);

    return streamHasAudio;
  }

  stopStreaming(cameraId: string) {
    const processInfo = this.processes.get(cameraId);
    if (processInfo) {
      this.logger.log(`Deteniendo stream de la cámara: ${cameraId}`);

      processInfo.process.kill('SIGTERM');
      setTimeout(() => {
        if (this.processes.has(cameraId)) {
          this.logger.warn(`FFmpeg de cámara ${cameraId} no respondió a SIGTERM, forzando SIGKILL`);
          processInfo.process.kill('SIGKILL');
        }
      }, 3000);

      this.processes.delete(cameraId);
      this.retryCounts.delete(cameraId);
    }
  }

  private handleRestart(cameraId: string, info: { rtspUrl: string; backendIp: string; videoPort: number; audioPort: number }) {
    const retries = this.retryCounts.get(cameraId) || 0;
    if (retries < this.MAX_RETRIES) {
      this.retryCounts.set(cameraId, retries + 1);
      this.logger.log(`Reintentando conexión con la cámara ${cameraId} en ${this.RETRY_DELAY_MS / 1000}s... (Intento ${retries + 1}/${this.MAX_RETRIES})`);
      setTimeout(() => {
        if (!this.processes.has(cameraId)) {
          this.startStreaming(cameraId, info.rtspUrl, info.backendIp, info.videoPort, info.audioPort);
        }
      }, this.RETRY_DELAY_MS);
    } else {
      this.logger.error(`Se alcanzó el número máximo de reintentos (${this.MAX_RETRIES}) para la cámara ${cameraId}.`);
      if (this.onStreamFailed) {
        this.onStreamFailed(cameraId);
      }
    }
  }

  stopAllStreams() {
    this.logger.log('Deteniendo todos los streams de FFmpeg activos hacia el backend...');
    for (const cameraId of this.processes.keys()) {
      this.stopStreaming(cameraId);
    }
  }

  onModuleDestroy() {
    this.logger.log('Deteniendo todos los streams de FFmpeg activos en la destrucción del módulo...');
    this.stopAllStreams();
  }
}
