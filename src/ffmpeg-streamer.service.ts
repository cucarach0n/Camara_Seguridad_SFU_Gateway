import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { spawn, exec, ChildProcess } from 'child_process';

@Injectable()
export class FfmpegStreamerService implements OnModuleDestroy {
  private readonly logger = new Logger(FfmpegStreamerService.name);
  private processes = new Map<string, { process: ChildProcess; rtspUrl: string; backendIp: string; videoPort: number; audioPort: number }>();
  private retryCounts = new Map<string, number>();
  private readonly MAX_RETRIES = 5;
  private readonly RETRY_DELAY_MS = 5000;

  // Verifica si el flujo RTSP contiene una pista de audio
  private async hasAudio(rtspUrl: string): Promise<boolean> {
    return new Promise((resolve) => {
      // Tiempo límite de 3 segundos para evitar bloqueos si la cámara no responde
      // Usamos -rtsp_transport tcp para garantizar fiabilidad en la conexión de sondeo
      const command = `ffprobe -rtsp_transport tcp -v error -select_streams a -show_entries stream=codec_type -of default=nw=1:nk=1 "${rtspUrl}"`;
      this.logger.log(`Probing RTSP audio streams (TCP): ${command}`);
      
      exec(command, { timeout: 8000 }, (err, stdout) => {
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
      // Tiempo límite de 3 segundos
      const command = `ffprobe -rtsp_transport tcp -v error -select_streams v:0 -show_entries stream=codec_name -of default=nw=1:nk=1 "${rtspUrl}"`;
      this.logger.log(`Probing RTSP video codec (TCP): ${command}`);
      
      exec(command, { timeout: 8000 }, (err, stdout) => {
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

    this.logger.log(`Iniciando FFmpeg para cámara ${cameraId}. Video codec: ${videoCodec}, Audio detectado: ${streamHasAudio}. Destino: ${backendIp} (Video: ${videoPort}, Audio: ${audioPort})`);

    // Construir argumentos dinámicamente según la presencia de audio y el tipo de códec de video
    const args = [
      '-rtsp_transport', 'tcp',
      '-re',
      '-i', rtspUrl,
      '-map', '0:v:0'
    ];

    // Para WebRTC, es MANDATORIO que el video sea H.264 Baseline Profile y sin B-frames.
    // Aunque la cámara sea H.264, si usa High Profile o tiene B-frames, congelará el frontend.
    this.logger.log(`Cámara ${cameraId} reporta códec: ${videoCodec}. Forzando transcodificación a H.264 Baseline sin B-frames para evitar congelamiento en WebRTC.`);
    args.push(
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-tune', 'zerolatency',
      '-pix_fmt', 'yuv420p',
      '-profile:v', 'baseline',
      '-level', '3.1',
      '-r', '30', // Fuerza 30fps para evitar el bug de 90k fps
      '-bf', '0' // Desactiva B-frames, crucial para WebRTC
    );

    args.push(
      '-f', 'rtp',
      '-ssrc', '11111',
      '-payload_type', '101',
      `rtp://${backendIp}:${videoPort}?pkt_size=1200`
    );

    if (streamHasAudio) {
      args.push(
        '-map', '0:a:0',
        '-c:a', 'libopus',
        '-ab', '128k',
        '-ar', '48000',
        '-ac', '2',
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

      if (processInfo && code !== 0 && code !== null) {
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
    }
  }

  onModuleDestroy() {
    this.logger.log('Deteniendo todos los streams de FFmpeg activos en la destrucción del módulo...');
    for (const cameraId of this.processes.keys()) {
      this.stopStreaming(cameraId);
    }
  }
}
