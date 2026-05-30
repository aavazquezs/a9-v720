#!/usr/bin/env python3

import sys
import os
import io
import time
import json
import socket
import threading
from datetime import datetime, date
from queue import Queue, Empty
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QCheckBox, QTabWidget,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QGroupBox, QFormLayout, QSplitter, QPlainTextEdit,
    QStatusBar, QToolBar, QMessageBox, QFileDialog,
    QCalendarWidget, QFrame, QGridLayout, QSizePolicy,
    QMenuBar, QMenu, QDialog, QDialogButtonBox,
    QListWidget, QListWidgetItem, QAbstractItemView,
    QScrollArea, QSpinBox, QComboBox
)
from PySide6.QtCore import (
    Qt, QThread, QObject, Signal, Slot, QTimer, QByteArray,
    QMutex, QMutexLocker, QSize, QCoreApplication
)
from PySide6.QtGui import (
    QAction, QImage, QPixmap, QFont, QPalette, QColor,
    QTextCursor, QCloseEvent, QIcon, QShortcut, QKeySequence
)

import numpy as np
from PIL import Image

from netcl_tcp import netcl_tcp
from v720_ap import v720_ap
from v720_sta import start_srv, v720_sta
from v720_http import v720_http
import cmd_udp
from prot_udp import prot_udp
from prot_json_udp import prot_json_udp
from prot_ap import prot_ap
from log import log
import logging

DEFAULT_HOST = "192.168.169.1"
DEFAULT_PORT = 6123
WAV_HEADER = b'RIFF\x8a\xdc\x01\x00WAVEfmt \x12\x00\x00\x00\x06\x00\x01\x00@\x1f\x00\x00@\x1f\x00\x00\x01\x00\x08\x00\x00\x00fact\x04\x00\x00\x006\xdc\x01\x00LIST\x1a\x00\x00\x00INFOISFT\x0e\x00\x00\x00Lavf58.45.100\x00data\xff\xff\xff\xff'


def jpeg_to_qimage(jpeg_bytes: bytes) -> QImage:
    img = QImage.fromData(QByteArray(jpeg_bytes))
    return img


def qimage_to_pixmap(img: QImage, max_size: QSize = None) -> QPixmap:
    pix = QPixmap.fromImage(img)
    if max_size and (pix.width() > max_size.width() or pix.height() > max_size.height()):
        pix = pix.scaled(max_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
    return pix


class GuiLogHandler(logging.Handler):
    def __init__(self, signal_target):
        super().__init__()
        self.signal_target = signal_target

    def emit(self, record):
        msg = self.format(record)
        self.signal_target.emit(msg)


class CameraStreamWorker(QObject):
    frame_ready = Signal(QImage)
    fps_changed = Signal(float)
    audio_frame = Signal(bytes)
    camera_info = Signal(dict)
    disconnected = Signal()
    error_occurred = Signal(str)
    motor_result = Signal(dict)
    connect_to = Signal(str, int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._running = False
        self._sock = None
        self._cam = None
        self._cmd_queue = Queue()

    def send_motor(self, direction: int, field: str, use_ap_req: bool = True):
        self._cmd_queue.put(('motor', direction, field, use_ap_req))

    @Slot(str, int)
    def start_connect(self, host: str, port: int):
        try:
            self._sock = netcl_tcp(host, port)
            self._sock.open()
            self._cam = v720_ap(self._sock)
            self._cam.init_live_motion()
            info = self._cam.baseinfo()
            sd = self._cam.sdcard_status()
            info_dict = {
                'version': info.content.get('version', '?'),
                'dev_id': self._cam.dev_id or '?',
                'sd_status': sd,
                'host': host,
                'port': port,
            }
            self.camera_info.emit(info_dict)
        except Exception as e:
            self.error_occurred.emit(f'Connection failed: {e}')

    @Slot()
    def start_stream(self):
        self._running = True
        sync = False
        frame = bytearray()
        last_frame_time = time.time()
        ping_timer = time.time()

        try:
            self._cam._ap_req({
                'code': cmd_udp.CODE_FORWARD_OPEN_A_OPEN_V,
                'devTarget': 'deadbeef',
            })

            while self._running:
                try:
                    while not self._cmd_queue.empty():
                        cmd, *args = self._cmd_queue.get_nowait()
                        if cmd == 'motor':
                            direction, field, use_ap_req = args
                            self._cam.set_motor_state_async(direction, field, use_ap_req)

                    now = time.time()
                    if now - ping_timer > 5:
                        try:
                            self._cam.ping()
                        except Exception:
                            pass
                        ping_timer = now

                    data = self._sock.recv()
                    if data is None or len(data) == 0:
                        break

                    pkg = prot_udp.resp(data)
                    if pkg is None:
                        continue

                    if pkg.cmd == cmd_udp.P2P_UDP_CMD_JPEG:
                        if not sync:
                            f = pkg.payload.find(b'\xff\xd8')
                            if f != -1:
                                g = pkg.payload.find(b'\xff\xd9', f)
                                if g != -1:
                                    frame = bytearray(pkg.payload[f:g + 2])
                                    img = jpeg_to_qimage(bytes(frame))
                                    if not img.isNull():
                                        self.frame_ready.emit(img)
                                        t = time.time()
                                        fps = round(1 / (t - last_frame_time), 1)
                                        self.fps_changed.emit(fps)
                                        last_frame_time = t
                                    frame.clear()
                                else:
                                    frame = bytearray(pkg.payload[f:])
                                    sync = True
                        else:
                            f = pkg.payload.find(b'\xff\xd9')
                            if f != -1:
                                frame.extend(pkg.payload[:f + 2])
                                img = jpeg_to_qimage(bytes(frame))
                                if not img.isNull():
                                    self.frame_ready.emit(img)
                                    t = time.time()
                                    fps = round(1 / (t - last_frame_time), 1)
                                    self.fps_changed.emit(fps)
                                    last_frame_time = t
                                frame.clear()
                                sync = False
                            else:
                                sofi = pkg.payload.find(b'\xff\xd8')
                                if sofi != -1:
                                    frame = bytearray(pkg.payload[sofi:])
                                else:
                                    frame.extend(pkg.payload)

                    elif pkg.cmd == cmd_udp.P2P_UDP_CMD_G711:
                        self.audio_frame.emit(bytes(pkg.payload))

                except socket.timeout:
                    break
                except OSError:
                    if not self._running:
                        break
                    continue

        except Exception as e:
            if self._running:
                self.error_occurred.emit(f'Stream error: {e}')
        finally:
            self._running = False
            self.disconnected.emit()

    @Slot()
    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass


class FileListWorker(QObject):
    dates_ready = Signal(list)
    files_ready = Signal(str, list)
    file_info_ready = Signal(dict)
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

    @Slot(str, int)
    def list_dates(self, host: str, port: int):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                if cam.sdcard_status():
                    dates = cam.sdcard_datelist()
                    self.dates_ready.emit(dates if dates else [])
                else:
                    self.error_occurred.emit('No SD card found')
        except Exception as e:
            self.error_occurred.emit(f'Failed to list dates: {e}')

    @Slot(str, int, str)
    def list_files(self, host: str, port: int, date_str: str):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                if not cam.sdcard_status():
                    self.error_occurred.emit('No SD card found')
                    return
                files = cam.filename_list(int(date_str))
                result = []
                for hour, minute in files:
                    info = cam.avi_file_info(int(date_str), hour, minute)
                    if info:
                        result.append({
                            'hour': hour,
                            'minute': minute,
                            'fileName': info.get('fileName', ''),
                            'fileSize': info.get('fileSize', 0),
                        })
                    else:
                        result.append({
                            'hour': hour,
                            'minute': minute,
                            'fileName': f'{date_str}-{hour:02d}-{minute:02d}.avi',
                            'fileSize': 0,
                        })
                self.files_ready.emit(date_str, result)
        except Exception as e:
            self.error_occurred.emit(f'Failed to list files: {e}')

    @Slot(str, int, int, int, str)
    def get_file_info(self, host: str, port: int, date: int, hour: int, minute: int):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                info = cam.avi_file_info(date, hour, minute)
                if info:
                    self.file_info_ready.emit(info)
                else:
                    self.error_occurred.emit('File not found')
        except Exception as e:
            self.error_occurred.emit(f'Failed to get file info: {e}')


class FileDownloadWorker(QObject):
    progress = Signal(int, int)
    finished = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

    @Slot(str, int, int, int, int, str)
    def download(self, host: str, port: int, date: int, hour: int, minute: int, dest: str):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()

                file_info = cam.avi_file_info(date, hour, minute)
                if not file_info or file_info.get('fileSize', -1) <= 0:
                    self.error_occurred.emit('File not found or empty')
                    return

                total = file_info['fileSize']
                downloaded = [0]

                def on_progress(pkg_id, sz):
                    downloaded[0] += sz
                    self.progress.emit(downloaded[0], total)

                result = cam.get_file(date, hour, minute, on_progress)
                if result and result[1]:
                    with open(dest, 'wb') as f:
                        f.write(result[1])
                    self.finished.emit(dest)
                else:
                    self.error_occurred.emit('Download failed')
        except Exception as e:
            self.error_occurred.emit(f'Download error: {e}')


class WiFiScanWorker(QObject):
    networks_ready = Signal(list)
    wifi_configured = Signal(bool)
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

    @Slot(str, int)
    def scan(self, host: str, port: int):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                resp = cam.wifi_scan()
                if resp and resp.content:
                    nets = resp.content.get('apList', [])
                    self.networks_ready.emit(nets if isinstance(nets, list) else [nets])
                else:
                    self.networks_ready.emit([])
        except Exception as e:
            self.error_occurred.emit(f'WiFi scan failed: {e}')

    @Slot(str, int, str, str)
    def set_wifi(self, host: str, port: int, ssid: str, pwd: str):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                result = cam.set_wifi(ssid, pwd)
                if result is not None:
                    cam.reboot()
                    self.wifi_configured.emit(True)
                else:
                    self.error_occurred.emit('Camera did not respond')
        except Exception as e:
            self.error_occurred.emit(f'WiFi config failed: {e}')


class CameraControlWorker(QObject):
    command_result = Signal(dict)
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

    @Slot(str, int, str, object)
    def send_command(self, host: str, port: int, cmd_name: str, value):
        try:
            with netcl_tcp(host, port) as sock:
                cam = v720_ap(sock)
                cam.init_live_motion()
                result = None
                if cmd_name == 'ir_led':
                    result = cam.ir_led(bool(value))
                elif cmd_name == 'flip':
                    result = cam.flip(bool(value))
                elif cmd_name == 'motor':
                    if isinstance(value, dict):
                        result = cam.set_motor_state(
                            int(value['direction']), value['field'],
                            value.get('use_ap_req', True))
                    else:
                        result = cam.set_motor_state(int(value))
                elif cmd_name == 'reboot':
                    cam.reboot()
                    result = {'status': 'ok'}
                if result:
                    self.command_result.emit(result.content if hasattr(result, 'content') else {'status': 'ok'})
        except Exception as e:
            self.error_occurred.emit(f'Command {cmd_name} failed: {e}')


class FakeServerRunner(QObject):
    device_connected = Signal(str, str, int)
    device_disconnected = Signal(str)
    started = Signal()
    stopped = Signal()
    error_occurred = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._http_port = 8080
        self._running = False
        self._thread = None
        self._known_devs = set()

    @Slot(int)
    def start(self, http_port: int = 8080):
        self._http_port = http_port
        self._known_devs.clear()

        def on_init(dev):
            self.device_connected.emit(dev.id, dev.host, dev.port)
            self._known_devs.add(dev.id)

        def on_disconnect(dev):
            self.device_disconnected.emit(dev.id)
            self._known_devs.discard(dev.id)

        def run():
            self._running = True
            self.started.emit()
            try:
                v720_sta.tcp_thread(self._http_port, on_init, on_disconnect)
            except Exception as e:
                self.error_occurred.emit(f'Server error: {e}')
            finally:
                self._running = False
                self.stopped.emit()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    @Slot()
    def stop(self):
        v720_sta.kill()


class VideoWidget(QLabel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(640, 480)
        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet('background-color: #1a1a1a; border: 1px solid #333;')
        self.setText('No video')
        self._fps_label = QLabel(self)
        self._fps_label.setStyleSheet(
            'background-color: rgba(0,0,0,160); color: #0f0; padding: 4px 8px; '
            'border-radius: 3px; font-family: monospace; font-size: 13px;'
        )
        self._fps_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self._fps_label.setText('FPS: -')
        self._fps_label.adjustSize()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fps_label.move(8, 8)

    def set_fps(self, fps: float):
        self._fps_label.setText(f'FPS: {fps:.1f}')
        self._fps_label.adjustSize()


class CameraInfoPanel(QGroupBox):
    def __init__(self, parent=None):
        super().__init__('Camera Info', parent)
        layout = QFormLayout(self)
        layout.setSpacing(4)

        self.lbl_version = QLabel('-')
        self.lbl_device_id = QLabel('-')
        self.lbl_sd_status = QLabel('-')
        self.lbl_host = QLabel('-')

        layout.addRow('Firmware:', self.lbl_version)
        layout.addRow('Device ID:', self.lbl_device_id)
        layout.addRow('SD Card:', self.lbl_sd_status)
        layout.addRow('Host:', self.lbl_host)

    def update_info(self, info: dict):
        self.lbl_version.setText(info.get('version', '-'))
        self.lbl_device_id.setText(str(info.get('dev_id', '-')))
        sd = info.get('sd_status')
        self.lbl_sd_status.setText('OK' if sd else 'Not found' if sd is False else '-')
        self.lbl_host.setText(f'{info.get("host", "-")}:{info.get("port", "-")}')


class ControlPanel(QGroupBox):
    ir_toggled = Signal(bool)
    flip_toggled = Signal(bool)
    reboot_requested = Signal()

    def __init__(self, parent=None):
        super().__init__('Controls', parent)
        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        self.chk_ir = QCheckBox('IR LED')
        self.chk_flip = QCheckBox('Flip Video')

        self.btn_reboot = QPushButton('Reboot Camera')
        self.btn_reboot.setStyleSheet(
            'QPushButton { background-color: #c0392b; color: white; font-weight: bold; }'
            'QPushButton:hover { background-color: #e74c3c; }'
        )

        layout.addWidget(self.chk_ir)
        layout.addWidget(self.chk_flip)
        layout.addStretch()
        layout.addWidget(self.btn_reboot)

        self.chk_ir.toggled.connect(self.ir_toggled)
        self.chk_flip.toggled.connect(self.flip_toggled)
        self.btn_reboot.clicked.connect(self.reboot_requested)

    def set_enabled(self, enabled: bool):
        self.chk_ir.setEnabled(enabled)
        self.chk_flip.setEnabled(enabled)
        self.btn_reboot.setEnabled(enabled)


class PTZPanel(QGroupBox):
    motor_start = Signal(int)
    motor_stop = Signal()

    def __init__(self, parent=None):
        super().__init__('PTZ Control', parent)
        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        grid = QGridLayout()
        grid.setSpacing(4)

        self.btn_up = QPushButton('\u25B2')
        self.btn_left = QPushButton('\u25C0')
        self.btn_home = QPushButton('\u25C9')
        self.btn_right = QPushButton('\u25B6')
        self.btn_down = QPushButton('\u25BC')

        for btn in [self.btn_up, self.btn_left, self.btn_home, self.btn_right, self.btn_down]:
            btn.setFixedSize(48, 48)
            btn.setStyleSheet(
                'QPushButton { background-color: #34495e; color: white; font-size: 18px; '
                'border-radius: 6px; font-weight: bold; }'
                'QPushButton:hover { background-color: #4a6a8a; }'
                'QPushButton:pressed { background-color: #2980b9; }'
            )

        grid.addWidget(self.btn_up, 0, 1)
        grid.addWidget(self.btn_left, 1, 0)
        grid.addWidget(self.btn_home, 1, 1)
        grid.addWidget(self.btn_right, 1, 2)
        grid.addWidget(self.btn_down, 2, 1)

        layout.addLayout(grid)

        import cmd_udp as _cmd
        dirs = {
            self.btn_up: _cmd.MOTOR_UP,
            self.btn_down: _cmd.MOTOR_DOWN,
            self.btn_left: _cmd.MOTOR_LEFT,
            self.btn_right: _cmd.MOTOR_RIGHT,
            self.btn_home: _cmd.MOTOR_CALIBRATE,
        }
        for btn, d in dirs.items():
            btn.pressed.connect(lambda d=d: self.motor_start.emit(d))
            btn.released.connect(self.motor_stop.emit)

    def set_enabled(self, enabled: bool):
        for btn in [self.btn_up, self.btn_left, self.btn_home, self.btn_right, self.btn_down]:
            btn.setEnabled(enabled)


class ConnectionBar(QFrame):
    connect_requested = Signal(str, int)
    disconnect_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.StyledPanel)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)

        self.txt_host = QLineEdit(DEFAULT_HOST)
        self.txt_host.setPlaceholderText('Host')
        self.txt_host.setFixedWidth(140)

        self.txt_port = QLineEdit(str(DEFAULT_PORT))
        self.txt_port.setPlaceholderText('Port')
        self.txt_port.setFixedWidth(70)

        self.btn_connect = QPushButton('Connect')
        self.btn_connect.setStyleSheet(
            'QPushButton { background-color: #27ae60; color: white; font-weight: bold; }'
            'QPushButton:hover { background-color: #2ecc71; }'
        )

        self.btn_disconnect = QPushButton('Disconnect')
        self.btn_disconnect.setEnabled(False)
        self.btn_disconnect.setStyleSheet(
            'QPushButton { background-color: #e67e22; color: white; }'
            'QPushButton:hover { background-color: #f39c12; }'
        )

        self.lbl_status = QLabel('Disconnected')
        self.lbl_status.setStyleSheet('color: #e74c3c; font-weight: bold;')

        layout.addWidget(QLabel('Camera:'))
        layout.addWidget(self.txt_host)
        layout.addWidget(self.txt_port)
        layout.addWidget(self.btn_connect)
        layout.addWidget(self.btn_disconnect)
        layout.addStretch()
        layout.addWidget(self.lbl_status)

        self.btn_connect.clicked.connect(self._on_connect)
        self.btn_disconnect.clicked.connect(self.disconnect_requested)

    def _on_connect(self):
        host = self.txt_host.text().strip()
        port = int(self.txt_port.text().strip())
        self.connect_requested.emit(host, port)

    def set_connected(self, connected: bool):
        self.btn_connect.setEnabled(not connected)
        self.btn_disconnect.setEnabled(connected)
        self.lbl_status.setText('Connected' if connected else 'Disconnected')
        self.lbl_status.setStyleSheet(
            'color: #2ecc71; font-weight: bold;' if connected else 'color: #e74c3c; font-weight: bold;'
        )


class LiveTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        self.video = VideoWidget()
        layout.addWidget(self.video, 1)

        ctrl_row = QHBoxLayout()
        self.btn_record = QPushButton('Record')
        self.btn_record.setCheckable(True)
        self.btn_record.setStyleSheet(
            'QPushButton:checked { background-color: #c0392b; color: white; font-weight: bold; }'
        )
        self.btn_snapshot = QPushButton('Snapshot')

        self.lbl_recording = QLabel('')
        self.lbl_recording.setStyleSheet('color: #e74c3c; font-weight: bold;')

        self.lbl_video_size = QLabel('')

        ctrl_row.addWidget(self.btn_record)
        ctrl_row.addWidget(self.btn_snapshot)
        ctrl_row.addWidget(self.lbl_recording)
        ctrl_row.addStretch()
        ctrl_row.addWidget(self.lbl_video_size)

        layout.addLayout(ctrl_row)

    def set_video_enabled(self, enabled: bool):
        self.btn_record.setEnabled(enabled)
        self.btn_snapshot.setEnabled(enabled)

    def recording_state(self, state: bool):
        self.btn_record.setChecked(state)
        self.lbl_recording.setText('REC' if state else '')


class RecordingsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        top_row = QHBoxLayout()

        self.calendar = QCalendarWidget()
        self.calendar.setMaximumDate(date.today())
        self.calendar.setGridVisible(True)
        top_row.addWidget(self.calendar, 1)

        right_panel = QVBoxLayout()
        self.btn_refresh = QPushButton('Refresh Files')
        self.tbl_files = QTableWidget(0, 4)
        self.tbl_files.setHorizontalHeaderLabels(['Hour', 'Minute', 'Filename', 'Size'])
        self.tbl_files.horizontalHeader().setStretchLastSection(True)
        self.tbl_files.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_files.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_files.verticalHeader().setVisible(False)

        self.btn_download = QPushButton('Download Selected')
        self.btn_download.setEnabled(False)
        self.btn_download.setStyleSheet(
            'QPushButton { background-color: #2980b9; color: white; font-weight: bold; }'
            'QPushButton:hover { background-color: #3498db; }'
        )

        self.progress = QProgressBar()
        self.progress.setVisible(False)

        right_panel.addWidget(self.btn_refresh)
        right_panel.addWidget(self.tbl_files, 1)
        right_panel.addWidget(self.btn_download)
        right_panel.addWidget(self.progress)

        top_row.addLayout(right_panel, 2)
        layout.addLayout(top_row, 1)

        self.lbl_status = QLabel('')
        layout.addWidget(self.lbl_status)


class SettingsTab(QWidget):
    motor_field_changed = Signal(str)
    ap_req_toggled = Signal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        ptz_group = QGroupBox('PTZ Configuration')
        ptz_layout = QFormLayout(ptz_group)
        self.cmb_motor_field = QComboBox()
        self.cmb_motor_field.addItems(['motorState', 'pirSysMode'])
        self.cmb_motor_field.setToolTip(
            'Field name used in the PTZ command payload.\n'
            '"pirSysMode" (default) from traffic capture.\n'
            '"motorState" from decompiled Java app.'
        )
        self.cmb_motor_field.currentTextChanged.connect(self.motor_field_changed.emit)
        ptz_layout.addRow('Motor Field:', self.cmb_motor_field)
        self.chk_ap_req = QCheckBox('Wrap in code 502 (AP protocol)')
        self.chk_ap_req.setChecked(True)
        self.chk_ap_req.setToolTip(
            'Uncheck to send motor commands directly without 502 wrapper,\n'
            'like the newer AP channel protocol does.'
        )
        self.chk_ap_req.toggled.connect(self.ap_req_toggled.emit)
        ptz_layout.addRow(self.chk_ap_req)
        layout.addWidget(ptz_group)

        wifi_group = QGroupBox('WiFi Configuration')
        wifi_layout = QVBoxLayout(wifi_group)

        scan_row = QHBoxLayout()
        self.btn_scan = QPushButton('Scan Networks')
        self.scan_progress = QLabel('')
        scan_row.addWidget(self.btn_scan)
        scan_row.addStretch()
        scan_row.addWidget(self.scan_progress)
        wifi_layout.addLayout(scan_row)

        self.tbl_networks = QTableWidget(0, 3)
        self.tbl_networks.setHorizontalHeaderLabels(['SSID', 'RSSI', 'Security'])
        self.tbl_networks.horizontalHeader().setStretchLastSection(True)
        self.tbl_networks.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_networks.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_networks.verticalHeader().setVisible(False)
        wifi_layout.addWidget(self.tbl_networks, 1)

        form = QFormLayout()
        self.txt_ssid = QLineEdit()
        self.txt_ssid.setPlaceholderText('Network SSID')
        self.txt_pwd = QLineEdit()
        self.txt_pwd.setEchoMode(QLineEdit.Password)
        self.txt_pwd.setPlaceholderText('Password')

        self.btn_connect_wifi = QPushButton('Connect Camera to WiFi')
        self.btn_connect_wifi.setStyleSheet(
            'QPushButton { background-color: #2980b9; color: white; font-weight: bold; }'
            'QPushButton:hover { background-color: #3498db; }'
        )

        form.addRow('SSID:', self.txt_ssid)
        form.addRow('Password:', self.txt_pwd)
        wifi_layout.addLayout(form)
        wifi_layout.addWidget(self.btn_connect_wifi)

        self.lbl_wifi_status = QLabel('')
        wifi_layout.addWidget(self.lbl_wifi_status)

        layout.addWidget(wifi_group)

        self.tbl_networks.itemClicked.connect(self._on_network_clicked)
        self.btn_connect_wifi.clicked.connect(self._on_connect_wifi)

    def _on_network_clicked(self, item):
        row = item.row()
        ssid = self.tbl_networks.item(row, 0).text()
        self.txt_ssid.setText(ssid)

    def _on_connect_wifi(self):
        ssid = self.txt_ssid.text().strip()
        pwd = self.txt_pwd.text().strip()
        if not ssid:
            QMessageBox.warning(self, 'WiFi', 'Enter an SSID')
            return
        if len(pwd) < 8:
            QMessageBox.warning(self, 'WiFi', 'Password must be at least 8 characters')
            return
        self.lbl_wifi_status.setText(f'Connecting to {ssid}...')
        self.lbl_wifi_status.setStyleSheet('color: #f39c12;')


class FakeServerTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        ctrl_row = QHBoxLayout()
        self.btn_start = QPushButton('Start Fake Server')
        self.btn_start.setStyleSheet(
            'QPushButton { background-color: #27ae60; color: white; font-weight: bold; }'
            'QPushButton:hover { background-color: #2ecc71; }'
        )
        self.btn_stop = QPushButton('Stop Server')
        self.btn_stop.setEnabled(False)
        self.btn_stop.setStyleSheet(
            'QPushButton { background-color: #c0392b; color: white; }'
            'QPushButton:hover { background-color: #e74c3c; }'
        )

        self.lbl_port = QLabel('HTTP Port:')
        self.spin_port = QSpinBox()
        self.spin_port.setRange(1024, 65535)
        self.spin_port.setValue(8080)

        ctrl_row.addWidget(self.btn_start)
        ctrl_row.addWidget(self.btn_stop)
        ctrl_row.addStretch()
        ctrl_row.addWidget(self.lbl_port)
        ctrl_row.addWidget(self.spin_port)

        layout.addLayout(ctrl_row)

        self.tbl_devices = QTableWidget(0, 4)
        self.tbl_devices.setHorizontalHeaderLabels(['UID', 'IP', 'Port', 'Actions'])
        self.tbl_devices.horizontalHeader().setStretchLastSection(True)
        self.tbl_devices.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_devices.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_devices.verticalHeader().setVisible(False)
        layout.addWidget(self.tbl_devices, 1)

        self.lbl_info = QLabel('Fake server not running')
        self.lbl_info.setStyleSheet('color: #7f8c8d; font-style: italic;')
        layout.addWidget(self.lbl_info)

    def set_running(self, running: bool):
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        self.spin_port.setEnabled(not running)


class LogPanel(QPlainTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setMaximumBlockCount(5000)
        self.setFont(QFont('Courier New', 9))
        self.setStyleSheet('background-color: #1e1e1e; color: #d4d4d4;')
        self.setFixedHeight(150)

    @Slot(str)
    def append_log(self, msg: str):
        self.appendPlainText(msg)
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.setTextCursor(cursor)


class A9MainWindow(QMainWindow):
    _worker_cmd = Signal(str, int, str, object)

    def __init__(self):
        super().__init__()
        self.setWindowTitle('A9 V720 Camera Controller')
        self.setMinimumSize(1100, 750)

        self._connected = False
        self._stream_active = False
        self._recording = False
        self._video_writer = None
        self._threads = []
        self._workers = []
        self._motor_field = 'motorState'
        self._use_ap_req = True
        self._ptz_timer = QTimer(self)
        self._ptz_timer.setInterval(50)
        self._ptz_timer.timeout.connect(self._ptz_send)
        self._ptz_direction = None

        self._setup_ui()
        self._setup_workers()
        self._setup_logging()

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.conn_bar = ConnectionBar()
        main_layout.addWidget(self.conn_bar)

        splitter = QSplitter(Qt.Horizontal)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(4, 4, 4, 4)

        self.info_panel = CameraInfoPanel()
        self.ctrl_panel = ControlPanel()
        self.ptz_panel = PTZPanel()

        left_layout.addWidget(self.info_panel)
        left_layout.addWidget(self.ctrl_panel)
        left_layout.addWidget(self.ptz_panel)
        left_layout.addStretch()
        left_panel.setFixedWidth(260)

        splitter.addWidget(left_panel)

        self.tabs = QTabWidget()
        self.live_tab = LiveTab()
        self.recordings_tab = RecordingsTab()
        self.settings_tab = SettingsTab()
        self.settings_tab.motor_field_changed.connect(
            lambda f: setattr(self, '_motor_field', f))
        self.settings_tab.ap_req_toggled.connect(
            lambda v: setattr(self, '_use_ap_req', v))
        self.fake_tab = FakeServerTab()

        self.tabs.addTab(self.live_tab, 'Live View')
        self.tabs.addTab(self.recordings_tab, 'Recordings')
        self.tabs.addTab(self.settings_tab, 'Settings')
        self.tabs.addTab(self.fake_tab, 'Fake Server')

        splitter.addWidget(self.tabs)
        splitter.setSizes([260, 840])
        main_layout.addWidget(splitter, 1)

        self.log_panel = LogPanel()
        main_layout.addWidget(self.log_panel)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage('Disconnected')

        self._setup_menu()

    def _setup_menu(self):
        mb = self.menuBar()
        file_menu = mb.addMenu('&File')
        act_save_log = QAction('Save Log As...', self)
        act_save_log.triggered.connect(self._save_log)
        file_menu.addAction(act_save_log)
        file_menu.addSeparator()
        act_exit = QAction('Exit', self)
        act_exit.setShortcut(QKeySequence.Quit)
        act_exit.triggered.connect(self.close)
        file_menu.addAction(act_exit)

        help_menu = mb.addMenu('&Help')
        act_about = QAction('About', self)
        act_about.triggered.connect(self._show_about)
        help_menu.addAction(act_about)

    def _save_log(self):
        path, _ = QFileDialog.getSaveFileName(self, 'Save Log', 'a9-gui.log', 'Log Files (*.log);;All Files (*)')
        if path:
            with open(path, 'w') as f:
                f.write(self.log_panel.toPlainText())

    def _show_about(self):
        QMessageBox.about(self, 'About A9 V720',
                          'A9 V720 Naxclow Camera Controller\n\n'
                          'PySide6 GUI for controlling A9 V720 IP cameras.\n'
                          'Based on the a9-v720 project by intx82.')

    def _setup_workers(self):
        self._stream_thread = QThread(self)
        self._stream_worker = CameraStreamWorker()
        self._stream_worker.moveToThread(self._stream_thread)
        self._stream_worker.connect_to.connect(self._stream_worker.start_connect)
        self._stream_thread.started.connect(lambda: None)
        self._stream_worker.frame_ready.connect(self._on_frame)
        self._stream_worker.fps_changed.connect(self._on_fps)
        self._stream_worker.camera_info.connect(self._on_camera_info)
        self._stream_worker.disconnected.connect(self._on_disconnected)
        self._stream_worker.error_occurred.connect(self._on_error)
        self._stream_thread.start()

        self._file_list_worker = FileListWorker()
        self._file_list_thread = QThread(self)
        self._file_list_worker.moveToThread(self._file_list_thread)
        self._file_list_thread.start()

        self._file_dl_worker = FileDownloadWorker()
        self._file_dl_thread = QThread(self)
        self._file_dl_worker.moveToThread(self._file_dl_thread)
        self._file_dl_thread.start()

        self._wifi_worker = WiFiScanWorker()
        self._wifi_thread = QThread(self)
        self._wifi_worker.moveToThread(self._wifi_thread)
        self._wifi_thread.start()

        self._ctrl_worker = CameraControlWorker()
        self._ctrl_thread = QThread(self)
        self._ctrl_worker.moveToThread(self._ctrl_thread)
        self._ctrl_thread.start()

        self._fake_runner = FakeServerRunner()

        self._setup_signals()

    def _setup_signals(self):
        self.conn_bar.connect_requested.connect(self._connect_camera)
        self.conn_bar.disconnect_requested.connect(self._disconnect_camera)

        self.ctrl_panel.ir_toggled.connect(self._toggle_ir)
        self.ctrl_panel.flip_toggled.connect(self._toggle_flip)
        self.ctrl_panel.reboot_requested.connect(self._reboot_camera)

        self.ptz_panel.motor_start.connect(self._ptz_start)
        self.ptz_panel.motor_stop.connect(self._ptz_stop)
        self._worker_cmd.connect(self._ctrl_worker.send_command)

        self.live_tab.btn_record.toggled.connect(self._toggle_record)
        self.live_tab.btn_snapshot.clicked.connect(self._take_snapshot)

        self._file_list_worker.dates_ready.connect(self._on_dates_ready)
        self._file_list_worker.files_ready.connect(self._on_files_ready)
        self._file_list_worker.error_occurred.connect(self._on_error)

        self._file_dl_worker.progress.connect(self._on_dl_progress)
        self._file_dl_worker.finished.connect(self._on_dl_finished)
        self._file_dl_worker.error_occurred.connect(self._on_error)

        self._wifi_worker.networks_ready.connect(self._on_networks_ready)
        self._wifi_worker.wifi_configured.connect(self._on_wifi_configured)
        self._wifi_worker.error_occurred.connect(self._on_error)

        self._ctrl_worker.command_result.connect(self._on_command_result)
        self._ctrl_worker.error_occurred.connect(self._on_error)

        self._fake_runner.device_connected.connect(self._on_fake_device)
        self._fake_runner.device_disconnected.connect(self._on_fake_device_left)
        self._fake_runner.started.connect(self._on_fake_started)
        self._fake_runner.stopped.connect(self._on_fake_stopped)
        self._fake_runner.error_occurred.connect(self._on_error)

        self.recordings_tab.calendar.clicked.connect(self._on_date_selected)
        self.recordings_tab.btn_refresh.clicked.connect(self._refresh_recordings)
        self.recordings_tab.tbl_files.itemSelectionChanged.connect(self._on_file_selected)
        self.recordings_tab.btn_download.clicked.connect(self._download_file)

        self.settings_tab.btn_scan.clicked.connect(self._scan_wifi)

        self.fake_tab.btn_start.clicked.connect(self._start_fake_server)
        self.fake_tab.btn_stop.clicked.connect(self._stop_fake_server)

    def _setup_logging(self):
        handler = GuiLogHandler(self.log_panel.append_log)
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', '%H:%M:%S'))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.DEBUG)

    def _connect_camera(self, host: str, port: int):
        self.log_panel.append_log(f'Connecting to {host}:{port}...')
        self.status_bar.showMessage(f'Connecting to {host}:{port}...')
        self._connected_host = host
        self._connected_port = port
        self._stream_worker.connect_to.emit(host, port)

    def _on_camera_info(self, info: dict):
        self._connected = True
        self.conn_bar.set_connected(True)
        self.info_panel.update_info(info)
        self.status_bar.showMessage(f'Connected to {info["host"]}:{info["port"]} | FW: {info["version"]}')
        self.log_panel.append_log(f'Connected. FW: {info["version"]}, SD: {info["sd_status"]}')
        self.ctrl_panel.set_enabled(True)
        self.ptz_panel.set_enabled(True)
        self.live_tab.set_video_enabled(True)

        QTimer.singleShot(100, self._stream_worker.start_stream)

    def _on_frame(self, img: QImage):
        if not self._stream_active:
            self._stream_active = True
        pix = qimage_to_pixmap(img, self.live_tab.video.size())
        self.live_tab.video.setPixmap(pix)

        if self._recording and self._video_writer is not None:
            arr = img.convertedTo(QImage.Format_RGB888)
            w, h = arr.width(), arr.height()
            bits = arr.bits()
            if isinstance(bits, memoryview):
                bits = bytes(bits)
            cv_img = np.frombuffer(bits, dtype=np.uint8).reshape((h, w, 3)).copy()
            self._video_writer.write(cv_img)
            sz = os.path.getsize(self._recording_path) if hasattr(self, '_recording_path') else 0
            if sz > 1024 * 1024:
                self.live_tab.lbl_video_size.setText(f'{sz // (1024 * 1024)} MB')
            else:
                self.live_tab.lbl_video_size.setText(f'{sz // 1024} KB')

    def _on_fps(self, fps: float):
        self.live_tab.video.set_fps(fps)

    def _on_disconnected(self):
        self._connected = False
        self._stream_active = False
        self._ptz_timer.stop()
        self._ptz_direction = None
        self.conn_bar.set_connected(False)
        self.ctrl_panel.set_enabled(False)
        self.ptz_panel.set_enabled(False)
        self.live_tab.set_video_enabled(False)
        self.live_tab.video.clear()
        self.live_tab.video.setText('Disconnected')
        self.status_bar.showMessage('Disconnected')
        self.log_panel.append_log('Disconnected from camera')

        if self._recording:
            self._stop_recording()

    def _on_error(self, msg: str):
        self.log_panel.append_log(f'ERROR: {msg}')
        self.status_bar.showMessage(f'Error: {msg}')

    def _disconnect_camera(self):
        self.log_panel.append_log('Disconnecting...')
        self._stream_worker.stop()

    def _toggle_ir(self, enabled: bool):
        self.log_panel.append_log(f'IR LED: {"ON" if enabled else "OFF"}')
        self._worker_cmd.emit(
            self._connected_host, self._connected_port, 'ir_led', enabled)

    def _toggle_flip(self, enabled: bool):
        self.log_panel.append_log(f'Flip: {"ON" if enabled else "OFF"}')
        self._worker_cmd.emit(
            self._connected_host, self._connected_port, 'flip', enabled)

    def _reboot_camera(self):
        reply = QMessageBox.question(self, 'Reboot', 'Reboot camera? Connection will be lost.',
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.log_panel.append_log('Rebooting camera...')
            self._worker_cmd.emit(
                self._connected_host, self._connected_port, 'reboot', None)
            QTimer.singleShot(2000, self._disconnect_camera)

    def _ptz_start(self, direction: int):
        if not self._connected:
            return
        self._ptz_direction = direction
        names = {0: 'HOME', 1: 'RIGHT', 2: 'LEFT', 3: 'UP', 4: 'DOWN'}
        self.log_panel.append_log(f'PTZ: {names.get(direction, str(direction))}')
        self._ptz_send()
        self._ptz_timer.start()

    def _ptz_stop(self):
        self._ptz_timer.stop()
        self._ptz_direction = None

    def _ptz_send(self):
        if not self._connected or self._ptz_direction is None:
            return
        self._stream_worker.send_motor(self._ptz_direction, self._motor_field, self._use_ap_req)

    def _on_command_result(self, result: dict):
        self.log_panel.append_log(f'Command OK: {result}')

    def _toggle_record(self, checked: bool):
        if checked:
            self._start_recording()
        else:
            self._stop_recording()

    def _start_recording(self):
        path, _ = QFileDialog.getSaveFileName(self, 'Save Recording', f'live_{datetime.now():%Y%m%d_%H%M%S}.avi',
                                              'AVI Files (*.avi);;All Files (*)')
        if not path:
            self.live_tab.recording_state(False)
            return

        import cv2
        self._recording_path = path
        self._video_writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'), 10, (640, 480))
        self._recording = True
        self.live_tab.recording_state(True)
        self.log_panel.append_log(f'Recording started: {path}')

    def _stop_recording(self):
        self._recording = False
        if self._video_writer:
            self._video_writer.release()
            self._video_writer = None
        if hasattr(self, '_recording_path') and self._recording_path:
            self.log_panel.append_log(f'Recording saved: {self._recording_path}')
        self.live_tab.recording_state(False)
        self.live_tab.lbl_video_size.setText('')

    def _take_snapshot(self):
        pix = self.live_tab.video.pixmap()
        if pix is None or pix.isNull():
            self.log_panel.append_log('No video to capture')
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Save Snapshot',
                                              f'snapshot_{datetime.now():%Y%m%d_%H%M%S}.jpg',
                                              'JPEG (*.jpg);;PNG (*.png)')
        if path:
            pix.save(path)
            self.log_panel.append_log(f'Snapshot saved: {path}')

    def _on_date_selected(self, qdate):
        if not self._connected:
            return
        date_str = qdate.toString('yyyyMMdd')
        self.recordings_tab.lbl_status.setText(f'Loading files for {date_str}...')
        self.log_panel.append_log(f'Loading files for {date_str}...')
        QTimer.singleShot(0, lambda: self._file_list_worker.list_files(
            self._connected_host, self._connected_port, date_str))

    def _refresh_recordings(self):
        if not self._connected:
            return
        self.log_panel.append_log('Refreshing recording dates...')
        QTimer.singleShot(0, lambda: self._file_list_worker.list_dates(
            self._connected_host, self._connected_port))

    def _on_dates_ready(self, dates: list):
        self.log_panel.append_log(f'Found {len(dates)} recording dates')
        self._recording_dates = dates
        if dates:
            self.recordings_tab.lbl_status.setText(f'{len(dates)} dates with recordings')

    def _on_files_ready(self, date_str: str, files: list):
        tbl = self.recordings_tab.tbl_files
        tbl.setRowCount(0)
        if not files:
            self.recordings_tab.lbl_status.setText(f'No files for {date_str}')
            return
        for f in files:
            row = tbl.rowCount()
            tbl.insertRow(row)
            tbl.setItem(row, 0, QTableWidgetItem(f'{f["hour"]:02d}'))
            tbl.setItem(row, 1, QTableWidgetItem(f'{f["minute"]:02d}'))
            tbl.setItem(row, 2, QTableWidgetItem(f['fileName']))
            sz = f.get('fileSize', 0)
            if sz > 1024 * 1024:
                tbl.setItem(row, 3, QTableWidgetItem(f'{sz // (1024 * 1024)} MB'))
            elif sz > 1024:
                tbl.setItem(row, 3, QTableWidgetItem(f'{sz // 1024} KB'))
            else:
                tbl.setItem(row, 3, QTableWidgetItem(f'{sz} B'))
        self.recordings_tab.lbl_status.setText(f'{len(files)} files on {date_str}')
        self.log_panel.append_log(f'Loaded {len(files)} files for {date_str}')

    def _on_file_selected(self):
        selected = self.recordings_tab.tbl_files.selectedItems()
        self.recordings_tab.btn_download.setEnabled(len(selected) > 0)

    def _download_file(self):
        tbl = self.recordings_tab.tbl_files
        row = tbl.currentRow()
        if row < 0:
            return
        hour = int(tbl.item(row, 0).text())
        minute = int(tbl.item(row, 1).text())
        fname = tbl.item(row, 2).text()

        qdate = self.recordings_tab.calendar.selectedDate()
        date_int = int(qdate.toString('yyyyMMdd'))

        dest, _ = QFileDialog.getSaveFileName(self, 'Download File', fname, 'AVI Files (*.avi);;All Files (*)')
        if not dest:
            return

        self.recordings_tab.progress.setVisible(True)
        self.recordings_tab.progress.setValue(0)
        self.recordings_tab.btn_download.setEnabled(False)
        self.log_panel.append_log(f'Downloading {fname}...')

        QTimer.singleShot(0, lambda: self._file_dl_worker.download(
            self._connected_host, self._connected_port, date_int, hour, minute, dest))

    def _on_dl_progress(self, current: int, total: int):
        pct = int((current / total) * 100) if total > 0 else 0
        self.recordings_tab.progress.setValue(pct)
        if current > 1024 * 1024:
            status = f'Downloading: {current // (1024 * 1024)} / {total // (1024 * 1024)} MB'
        else:
            status = f'Downloading: {current // 1024} / {total // 1024} KB'
        self.recordings_tab.lbl_status.setText(status)

    def _on_dl_finished(self, path: str):
        self.recordings_tab.progress.setVisible(False)
        self.recordings_tab.btn_download.setEnabled(True)
        self.recordings_tab.lbl_status.setText(f'Downloaded to {path}')
        self.log_panel.append_log(f'Download complete: {path}')
        QMessageBox.information(self, 'Download', f'File saved to:\n{path}')

    def _scan_wifi(self):
        if not self._connected:
            return
        self.settings_tab.scan_progress.setText('Scanning...')
        self.settings_tab.btn_scan.setEnabled(False)
        self.log_panel.append_log('Scanning WiFi networks...')
        QTimer.singleShot(0, lambda: self._wifi_worker.scan(
            self._connected_host, self._connected_port))

    def _on_networks_ready(self, networks: list):
        self.settings_tab.scan_progress.setText('')
        self.settings_tab.btn_scan.setEnabled(True)
        tbl = self.settings_tab.tbl_networks
        tbl.setRowCount(0)

        if not networks:
            self.log_panel.append_log('No networks found')
            return

        for net in networks:
            row = tbl.rowCount()
            tbl.insertRow(row)
            tbl.setItem(row, 0, QTableWidgetItem(net.get('ssid', '?')))
            rssi = net.get('rssi', '?')
            tbl.setItem(row, 1, QTableWidgetItem(str(rssi)))
            tbl.setItem(row, 2, QTableWidgetItem(net.get('enc', '?')))

        self.log_panel.append_log(f'Found {len(networks)} networks')

    def _on_wifi_configured(self, success: bool):
        if success:
            self.settings_tab.lbl_wifi_status.setText('WiFi configured! Camera rebooting...')
            self.settings_tab.lbl_wifi_status.setStyleSheet('color: #2ecc71; font-weight: bold;')
            self.log_panel.append_log('WiFi configured, camera rebooting')
            QTimer.singleShot(3000, self._disconnect_camera)

    def _start_fake_server(self):
        port = self.fake_tab.spin_port.value()
        self.log_panel.append_log(f'Starting fake server on port {port}...')
        self.fake_tab.set_running(True)
        self.fake_tab.lbl_info.setText(f'Fake server running on port {port}')
        self.fake_tab.tbl_devices.setRowCount(0)

        def start():
            self._fake_runner.start(port)

        QTimer.singleShot(0, start)

    def _stop_fake_server(self):
        self.log_panel.append_log('Stopping fake server...')
        self._fake_runner.stop()
        self.fake_tab.set_running(False)
        self.fake_tab.lbl_info.setText('Fake server stopped')

    def _on_fake_started(self):
        self.log_panel.append_log('Fake server started')
        self.status_bar.showMessage('Fake server running')

    def _on_fake_stopped(self):
        self.log_panel.append_log('Fake server stopped')
        self.status_bar.showMessage('Fake server stopped')

    def _on_fake_device(self, uid: str, host: str, port: int):
        tbl = self.fake_tab.tbl_devices
        for i in range(tbl.rowCount()):
            if tbl.item(i, 0).text() == uid:
                return
        row = tbl.rowCount()
        tbl.insertRow(row)
        tbl.setItem(row, 0, QTableWidgetItem(uid))
        tbl.setItem(row, 1, QTableWidgetItem(host))
        tbl.setItem(row, 2, QTableWidgetItem(str(port)))

        btn_box = QWidget()
        btn_layout = QHBoxLayout(btn_box)
        btn_layout.setContentsMargins(2, 2, 2, 2)
        btn_live = QPushButton('Live')
        btn_snap = QPushButton('Snapshot')
        http_port = self.fake_tab.spin_port.value()

        btn_live.clicked.connect(lambda: self._open_fake_stream(uid, http_port, 'live'))
        btn_snap.clicked.connect(lambda: self._open_fake_stream(uid, http_port, 'snapshot'))

        btn_layout.addWidget(btn_live)
        btn_layout.addWidget(btn_snap)
        tbl.setCellWidget(row, 3, btn_box)

        self.log_panel.append_log(f'Device connected: {uid} @ {host}:{port}')

    def _on_fake_device_left(self, uid: str):
        tbl = self.fake_tab.tbl_devices
        for i in range(tbl.rowCount()):
            if tbl.item(i, 0).text() == uid:
                tbl.removeRow(i)
                break
        self.log_panel.append_log(f'Device disconnected: {uid}')

    def _open_fake_stream(self, uid: str, http_port: int, mode: str):
        import webbrowser
        url = f'http://localhost:{http_port}/dev/{uid}/{mode}'
        self.log_panel.append_log(f'Opening: {url}')
        webbrowser.open(url)

    def closeEvent(self, event: QCloseEvent):
        self.log_panel.append_log('Shutting down...')

        if self._recording:
            self._stop_recording()

        self._stream_worker.stop()
        self._stream_thread.quit()
        self._stream_thread.wait(3000)

        self._file_list_thread.quit()
        self._file_list_thread.wait(2000)
        self._file_dl_thread.quit()
        self._file_dl_thread.wait(2000)
        self._wifi_thread.quit()
        self._wifi_thread.wait(2000)
        self._ctrl_thread.quit()
        self._ctrl_thread.wait(2000)

        self._fake_runner.stop()

        event.accept()


def main():
    logging.basicConfig(level=logging.WARN)
    log.set_log_lvl(logging.WARN)

    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(53, 53, 53))
    palette.setColor(QPalette.WindowText, Qt.white)
    palette.setColor(QPalette.Base, QColor(35, 35, 35))
    palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
    palette.setColor(QPalette.ToolTipBase, QColor(25, 25, 25))
    palette.setColor(QPalette.ToolTipText, Qt.white)
    palette.setColor(QPalette.Text, Qt.white)
    palette.setColor(QPalette.Button, QColor(53, 53, 53))
    palette.setColor(QPalette.ButtonText, Qt.white)
    palette.setColor(QPalette.BrightText, Qt.red)
    palette.setColor(QPalette.Highlight, QColor(142, 45, 197).lighter())
    palette.setColor(QPalette.HighlightedText, Qt.black)
    app.setPalette(palette)

    win = A9MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
