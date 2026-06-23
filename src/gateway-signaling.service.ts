import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { io, Socket } from 'socket.io-client';
import { FfmpegStreamerService } from './ffmpeg-streamer.service';

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
      reconnection: true
    });

    this.socket.on('connect', () => {
      this.logger.log('Conectado con éxito al backend principal. Registrando Gateway...');
      
      // Registrar este cliente como una puerta de enlace de medios local
      this.socket.emit('register-gateway', {
        gatewayId: process.env.GATEWAY_ID || 'local-gateway-1',
        cameras: [
          // Ejemplo de cámaras que gestiona este gateway local.
          // En producción esto podría venir de una base de datos o archivo de configuración local.
          { id: 'camara-patio', name: 'Cámara Patio Trasero', rtspUrl: process.env.RTSP_CAMARA_PATIO || 'rtsp://admin:admin123@192.168.1.50:554/stream1' },
          { id: 'camara-entrada', name: 'Cámara Entrada Principal', rtspUrl: process.env.RTSP_CAMARA_ENTRADA || 'rtsp://admin:admin123@192.168.1.51:554/stream1' }
        ]
      });
    });

    this.socket.on('disconnect', () => {
      this.logger.warn('Desconectado del backend principal. Intentando reconectar...');
    });

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
  }

  onModuleDestroy() {
    if (this.socket) {
      this.socket.disconnect();
    }
  }
}
