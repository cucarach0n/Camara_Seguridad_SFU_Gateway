import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { FfmpegStreamerService } from './ffmpeg-streamer.service';
import { GatewaySignalingService } from './gateway-signaling.service';

@Module({
  imports: [],
  controllers: [AppController],
  providers: [
    AppService,
    FfmpegStreamerService,
    GatewaySignalingService
  ],
})
export class AppModule {}

