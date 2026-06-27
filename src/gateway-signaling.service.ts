import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { io, Socket } from 'socket.io-client';
import { FfmpegStreamerService } from './ffmpeg-streamer.service';
import { exec } from 'child_process';
import * as net from 'net';

@Injectable()
export class GatewaySignalingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(GatewaySignalingService.name);
  private socket: Socket;

  constructor(private ffmpegStreamerService: FfmpegStreamerService) {}

  onModuleInit() {
    const backendUrl = process.env.BACKEND_URL || 'http://localhost:3000';
    this.logger.log(`Conectando al servidor de señalización principal en: ${backendUrl}`);

    this.socket = io(backendUrl, {
      transports: ['websocket'],
      autoConnect: true,
      reconnection: true,
      auth: { gatewayId: process.env.GATEWAY_ID || 'local-gateway-1' }
    });

    this.socket.on('connect', () => {
      this.logger.log('Conectado con éxito al backend principal. Registrando Gateway...');
      
      // Registrar este cliente como una puerta de enlace de medios local
      // Las cámaras ahora se gestionan desde el backend BD
      this.socket.emit('register-gateway');
    });

    this.socket.on('disconnect', (reason) => {
      this.logger.warn(`Desconectado del backend principal: ${reason}`);
    });

    this.ffmpegStreamerService.onStreamFailed = (cameraId) => {
      if (this.socket && this.socket.connected) {
        this.socket.emit('gateway-stream-failed', { cameraId });
      }
    };

    // Escuchar solicitudes de activación de streams de cámara
    this.socket.on('start-rtsp-stream', async (
      data: { cameraId: string; rtspUrl: string; videoPort: number; audioPort: number; backendIp: string },
      ack
    ) => {
      this.logger.log(`Recibida orden de inicio de streaming: ${JSON.stringify(data)}`);
      
      let backendHost = data.backendIp || '127.0.0.1';
      if (backendHost === '0.0.0.0' && process.env.BACKEND_URL) {
        try {
          const url = new URL(process.env.BACKEND_URL);
          backendHost = url.hostname;
          this.logger.log(`backendIp era 0.0.0.0. Usando hostname del BACKEND_URL: ${backendHost}`);
        } catch (e) {
          this.logger.warn(`No se pudo parsear BACKEND_URL: ${process.env.BACKEND_URL}`);
        }
      }
      try {
        const hasAudio = await this.ffmpegStreamerService.startStreaming(
          data.cameraId,
          data.rtspUrl,
          backendHost,
          data.videoPort,
          data.audioPort
        );
        if (typeof ack === 'function') {
          ack({ hasAudio });
        }
      } catch (err) {
        this.logger.error(`Error al iniciar stream de cámara ${data.cameraId}:`, err);
        if (typeof ack === 'function') {
          ack({ hasAudio: false, error: err.message });
        }
      }
    });


    // Escuchar solicitudes de detención
    this.socket.on('stop-rtsp-stream', (data: { cameraId: string }) => {
      this.logger.log(`Recibida orden de detención de streaming para: ${data.cameraId}`);
      this.ffmpegStreamerService.stopStreaming(data.cameraId);
    });

    // Escuchar solicitudes de sondeo (ping)
    this.socket.on('probe-cameras', async (cameras: Array<{ id: string, rtspUrl: string }>) => {
      this.logger.log(`Validando disponibilidad RTSP de ${cameras.length} cámaras...`);
      const results = await Promise.all(cameras.map(async (cam) => {
        return new Promise<{id: string, isOnline: boolean}>((resolve) => {
          try {
            // Usar ffprobe para comprobar el stream. Esto maneja correctamente la autenticación (evita falsos 401)
            // y valida si realmente hay un stream disponible y no solo un puerto abierto.
            const timeoutMs = 4000;
            const timeoutUs = timeoutMs * 1000;
            const cmd = `ffprobe -v error -show_entries format=format_name -of default=noprint_wrappers=1:nokey=1 -rtsp_transport tcp -timeout ${timeoutUs} -i "${cam.rtspUrl}"`;
            
            exec(cmd, { timeout: timeoutMs + 1000 }, (error, stdout, stderr) => {
              if (error) {
                this.logger.debug(`[PROBE ${cam.id}] Offline: ${error.message.split('\\n')[0]}`);
                resolve({ id: cam.id, isOnline: false });
              } else {
                const output = stdout.trim();
                this.logger.debug(`[PROBE ${cam.id}] Output: ${output}`);
                if (output.includes('rtp') || output.includes('rtsp')) {
                  resolve({ id: cam.id, isOnline: true });
                } else {
                  resolve({ id: cam.id, isOnline: false });
                }
              }
            });
          } catch(e) {
            this.logger.debug(`[PROBE ${cam.id}] Excepción: ${e.message}`);
            resolve({ id: cam.id, isOnline: false });
          }
        });
      }));
      
      this.socket.emit('cameras-status', results);
    });
  }

  onModuleDestroy() {
    if (this.socket) {
      this.socket.disconnect();
    }
  }
}
