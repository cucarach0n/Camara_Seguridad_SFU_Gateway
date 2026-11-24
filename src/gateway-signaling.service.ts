import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { io, Socket } from 'socket.io-client';
import { FfmpegStreamerService } from './ffmpeg-streamer.service';
import { exec } from 'child_process';
import * as net from 'net';

@Injectable()
export class GatewaySignalingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(GatewaySignalingService.name);
  private socket: Socket;

  constructor(private ffmpegStreamerService: FfmpegStreamerService) { }

  /**
   * Comprueba si el host y puerto RTSP de la cámara (por defecto 554) responden a nivel TCP.
   * Es instantáneo (5-10ms), consume 0% CPU y no abre sesiones multimedia que desincronicen
   * el protocolo RTSP / CSeq de la cámara.
   */
  private checkCameraTcp(rtspUrl: string, timeoutMs = 2500): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      try {
        const parsed = new URL(rtspUrl);
        const host = parsed.hostname;
        const port = parsed.port ? parseInt(parsed.port, 10) : 554;

        let resolved = false;
        const socket = net.createConnection({ host, port });

        const finish = (isOnline: boolean) => {
          if (!resolved) {
            resolved = true;
            socket.destroy();
            resolve(isOnline);
          }
        };

        socket.setTimeout(timeoutMs);
        socket.once('connect', () => finish(true));
        socket.once('timeout', () => finish(false));
        socket.once('error', () => finish(false));
      } catch (err) {
        resolve(false);
      }
    });
  }

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
      if (!Array.isArray(cameras) || cameras.length === 0) return;

      const results = await Promise.all(cameras.map(async (cam) => {
            const camIdStr = String(cam.id);

            // 1. Si la cámara ya está transmitiendo activamente con FFmpeg, está online
            if (this.ffmpegStreamerService.isStreamActive(camIdStr)) {
              return { id: camIdStr, isOnline: true };
            }

        // 2. Si no está transmitiendo, verificar si el puerto TCP RTSP está abierto (sin abrir sesión de video)
        const isOnline = await this.checkCameraTcp(cam.rtspUrl);
        return { id: camIdStr, isOnline };
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
