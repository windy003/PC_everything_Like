import sys
import os
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLineEdit, QPushButton, QTableWidget, 
                            QTableWidgetItem, QHeaderView, QFileDialog, QMenuBar,
                            QMenu, QMessageBox, QLabel, QProgressBar)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
import sqlite3
import win32file
import win32con
import winerror
from PyQt6.QtGui import QKeySequence, QShortcut

class FastIndexWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, db_path, drives):
        super().__init__()
        self.db_path = db_path
        self.drives = drives

    def create_usn_journal(self, volume_handle):
        """创建或获取 USN Journal"""
        try:
            # 创建/获取 USN Journal
            journal_data = win32file.DeviceIoControl(
                volume_handle,
                win32file.FSCTL_QUERY_USN_JOURNAL,
                None,
                100
            )
            return journal_data
        except:
            # 如果 Journal 不存在，创建一个
            create_data = win32file.DeviceIoControl(
                volume_handle,
                win32file.FSCTL_CREATE_USN_JOURNAL,
                None,
                100
            )
            journal_data = win32file.DeviceIoControl(
                volume_handle,
                win32file.FSCTL_QUERY_USN_JOURNAL,
                None,
                100
            )
            return journal_data

    def read_usn_journal(self, volume_handle, drive, journal_data):
        """读取 USN Journal 记录"""
        usn = journal_data['FirstUsn']
        while True:
            try:
                # 读取 USN 记录
                data = win32file.DeviceIoControl(
                    volume_handle,
                    win32file.FSCTL_READ_USN_JOURNAL,
                    usn.to_bytes(8, 'little') + journal_data['NextUsn'].to_bytes(8, 'little'),
                    100
                )
                
                if not data:
                    break
                    
                # 解析记录
                usn_records = data[0]
                for record in usn_records:
                    # 跳过目录
                    if record.FileAttributes & win32con.FILE_ATTRIBUTE_DIRECTORY:
                        continue
                    
                    # 跳过回收站
                    if "$Recycle.Bin" in record.FileName:
                        continue
                        
                    try:
                        # 获取文件的完整路径
                        filename = record.FileName
                        # 使用 FindFiles 获取实际路径
                        search_path = f"{drive}*{filename}"
                        found_files = win32file.FindFiles(search_path)
                        
                        for file_info in found_files:
                            full_path = os.path.join(drive, file_info[8])
                            if os.path.exists(full_path) and not os.path.isdir(full_path):
                                yield (full_path, filename)
                                break
                                
                    except (WindowsError, Exception):
                        continue
                
                usn = data[1]  # 更新 USN
                
            except win32file.error as e:
                if e.winerror == winerror.ERROR_NO_MORE_ITEMS:
                    break
                raise

    def run(self):
        conn = sqlite3.connect(self.db_path)
        try:
            # 创建表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    filename TEXT,
                    size INTEGER,
                    modified_time TEXT
                )
            """)
            
            for drive in self.drives:
                if not drive.endswith(':\\'):
                    continue
                    
                try:
                    # 获取驱动器句柄
                    volume_handle = win32file.CreateFile(
                        f"\\\\?\\{drive}",
                        win32con.GENERIC_READ | win32con.GENERIC_WRITE,
                        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE,
                        None,
                        win32con.OPEN_EXISTING,
                        0,
                        None
                    )
                    
                    # 检查文件系统类型
                    fstype = win32file.GetVolumeInformation(drive)[4]
                    if fstype != "NTFS":
                        self.progress.emit(f"{drive} 不是 NTFS 文件系统，跳过...")
                        continue
                    
                    # 获取/创建 USN Journal
                    journal_data = self.create_usn_journal(volume_handle)
                    self.progress.emit(f"正在读取 {drive} 的 USN Journal...")
                    
                    # 批量处理
                    batch = []
                    batch_size = 1000
                    
                    # 读取 USN Journal 记录
                    for full_path, filename in self.read_usn_journal(volume_handle, drive, journal_data):
                        if self.isInterruptionRequested():
                            raise InterruptedError("索引过程被用户终止")
                        
                        try:
                            if os.path.exists(full_path):  # 确保文件存在
                                stats = os.stat(full_path)
                                batch.append((
                                    full_path,
                                    filename,
                                    stats.st_size,
                                    datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                                ))
                                
                                if len(batch) >= batch_size:
                                    conn.executemany(
                                        "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
                                        batch
                                    )
                                    conn.commit()
                                    batch = []
                                    
                        except (OSError, FileNotFoundError):
                            continue
                    
                    # 处理剩余的批次
                    if batch:
                        conn.executemany(
                            "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
                            batch
                        )
                        conn.commit()
                    
                except Exception as e:
                    self.progress.emit(f"处理驱动器 {drive} 时出错: {str(e)}")
                    continue
                    
                finally:
                    win32file.CloseHandle(volume_handle)
                    
        except Exception as e:
            self.progress.emit(f"索引过程出错: {str(e)}")
        finally:
            conn.close()
            self.finished.emit()

class EverythingGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_path = None
        self.conn = None
        self.initUI()
        self.init_database()

    def init_database(self):
        # 如果没有选择数据库，创建一个默认的
        if not self.db_path:
            self.db_path = "file_index.db"
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()

    def create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                filename TEXT,
                size INTEGER,
                modified_time TEXT
            )
        """)
        self.conn.commit()

    def initUI(self):
        self.setWindowTitle('Python Everything')
        self.setGeometry(100, 100, 800, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 数据库管理区域
        db_layout = QHBoxLayout()
        self.db_label = QLineEdit()
        self.db_label.setReadOnly(True)
        self.db_label.setPlaceholderText('当前数据库: file_index.db')
        
        self.select_db_btn = QPushButton('选择数据库(&S)')
        self.select_db_btn.clicked.connect(self.select_database)
        
        self.new_db_btn = QPushButton('新建数据库(&N)')
        self.new_db_btn.clicked.connect(self.create_new_database)
        
        self.reset_db_btn = QPushButton('重置数据库(&R)')
        self.reset_db_btn.clicked.connect(self.reset_database)

        db_layout.addWidget(self.db_label)
        db_layout.addWidget(self.select_db_btn)
        db_layout.addWidget(self.new_db_btn)
        db_layout.addWidget(self.reset_db_btn)
        layout.addLayout(db_layout)

        # 搜索区域
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('输入搜索关键词...')
        self.search_input.textChanged.connect(self.search_files)
        
        # 添加快捷键 Alt+D 聚焦到搜索框
        shortcut = QShortcut(QKeySequence("Alt+D"), self)
        shortcut.activated.connect(self.focus_search)
        
        self.index_btn = QPushButton('选择索引目录(&I)')
        self.index_btn.clicked.connect(self.select_directory)

        search_layout.addWidget(self.search_input)
        search_layout.addWidget(self.index_btn)
        layout.addLayout(search_layout)

        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(4)
        self.result_table.setHorizontalHeaderLabels(['文件名', '路径', '大小', '修改时间'])
        header = self.result_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self.result_table)

        # 添加菜单栏
        menubar = self.menuBar()
        
        # 索引菜单
        index_menu = menubar.addMenu('索引(&I)')
        
        # 添加索引选项
        index_drive_action = index_menu.addAction('索引所有驱动器(&A)')
        index_drive_action.setShortcut('Ctrl+A')
        index_drive_action.triggered.connect(self.index_all_drives)
        
        index_dir_action = index_menu.addAction('索引指定目录(&D)')
        index_dir_action.setShortcut('Ctrl+D')
        index_dir_action.triggered.connect(self.select_directory)
        
        # 添加分隔线
        index_menu.addSeparator()
        
        # 添加停止索引选项
        self.stop_index_action = index_menu.addAction('停止索引(&S)')
        self.stop_index_action.setShortcut('Ctrl+S')
        self.stop_index_action.triggered.connect(self.stop_indexing)
        self.stop_index_action.setEnabled(False)

        # 添加状态栏
        self.status_label = QLabel()
        self.statusBar().addWidget(self.status_label)
        
        # 添加进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.hide()  # 默认隐藏
        self.statusBar().addPermanentWidget(self.progress_bar)

    def select_database(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "选择数据库文件",
            "",
            "SQLite数据库 (*.db);;所有文件 (*.*)"
        )
        if file_name:
            if self.conn:
                self.conn.close()
            self.db_path = file_name
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()
            self.db_label.setText(f'当前数据库: {os.path.basename(self.db_path)}')

    def create_new_database(self):
        file_name, _ = QFileDialog.getSaveFileName(
            self,
            "新建数据库文件",
            "",
            "SQLite数据库 (*.db);;所有文件 (*.*)"
        )
        if file_name:
            if self.conn:
                self.conn.close()
            self.db_path = file_name
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()
            self.db_label.setText(f'当前数据库: {os.path.basename(self.db_path)}')

    def reset_database(self):
        if self.conn:
            self.conn.execute("DROP TABLE IF EXISTS files")
            self.create_tables()
            self.result_table.setRowCount(0)
            self.db_label.setText(f'当前数据库: {os.path.basename(self.db_path)} (已重置)')

    def select_directory(self):
        directory = QFileDialog.getExistingDirectory(self, "选择要索引的目录")
        if directory:
            try:
                drive = os.path.splitdrive(directory)[0] + '\\'
                
                # 显示确认对话框
                reply = QMessageBox.question(
                    self,
                    "确认索引",
                    f"确定要索引驱动器 {drive} 吗？\n这可能需要一些时间。",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.Yes:
                    self.index_btn.setEnabled(False)
                    self.stop_index_action.setEnabled(True)
                    
                    # 更新状态
                    self.status_label.setText(f"准备索引驱动器 {drive}...")
                    self.progress_bar.show()
                    
                    # 创建并启动工作线程
                    self.worker = FastIndexWorker(self.db_path, [drive])
                    self.worker.progress.connect(self.update_index_status)
                    self.worker.finished.connect(self.indexing_finished)
                    self.worker.start()
            
            except Exception as e:
                QMessageBox.warning(
                    self,
                    "错误",
                    f"索引过程出错: {str(e)}",
                    QMessageBox.StandardButton.Ok
                )
                self.indexing_finished()

    def update_index_status(self, status):
        """更新索引状态"""
        self.status_label.setText(status)
        self.progress_bar.show()
        self.progress_bar.setFormat(status)  # 在进度条上显示状态文本
        
        # 让进度条显示忙碌状态
        self.progress_bar.setRange(0, 0)
        
        # 更新按钮文本
        self.index_btn.setText("正在索引...")
        
        # 更新窗口标题
        self.setWindowTitle('Python Everything - 正在索引...')

    def indexing_finished(self):
        """索引完成后的处理"""
        self.index_btn.setEnabled(True)
        self.index_btn.setText("选择索引目录(&I)")
        self.stop_index_action.setEnabled(False)
        
        # 隐藏进度条
        self.progress_bar.hide()
        
        # 恢复窗口标题
        self.setWindowTitle('Python Everything')
        
        # 显示完成消息
        self.status_label.setText("索引完成！")
        
        # 弹出通知
        QMessageBox.information(
            self,
            "索引完成",
            "文件索引已完成！\n现在可以搜索文件了。",
            QMessageBox.StandardButton.Ok
        )

    def search_files(self):
        keyword = self.search_input.text()
        if not keyword:
            self.result_table.setRowCount(0)
            return

        cursor = self.conn.execute(
            "SELECT * FROM files WHERE filename LIKE ? LIMIT 100",
            (f"%{keyword}%",)
        )
        results = cursor.fetchall()

        self.result_table.setRowCount(len(results))
        for row, (path, filename, size, modified_time) in enumerate(results):
            self.result_table.setItem(row, 0, QTableWidgetItem(filename))
            self.result_table.setItem(row, 1, QTableWidgetItem(path))
            self.result_table.setItem(row, 2, QTableWidgetItem(f"{size:,} bytes"))
            self.result_table.setItem(row, 3, QTableWidgetItem(modified_time))

    def index_all_drives(self):
        """索引所有可用驱动器"""
        drives = []
        if os.name == 'nt':  # Windows系统
            import win32api
            drives = win32api.GetLogicalDriveStrings()
            drives = drives.split('\000')[:-1]
        else:  # Linux/Mac系统
            drives = ['/']
        
        reply = QMessageBox.question(self, '确认',
                                   f'确定要索引以下驱动器吗？\n{", ".join(drives)}',
                                   QMessageBox.StandardButton.Yes |
                                   QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            self.index_btn.setEnabled(False)
            self.index_btn.setText("正在索引驱动器...")
            self.stop_index_action.setEnabled(True)
            
            # 使用 FastIndexWorker
            self.worker = FastIndexWorker(self.db_path, drives)
            self.worker.progress.connect(self.update_index_status)
            self.worker.finished.connect(self.indexing_finished)
            self.worker.start()

    def stop_indexing(self):
        """停止索引过程"""
        if hasattr(self, 'worker') and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "确认停止",
                "确定要停止索引过程吗？\n已索引的文件将被保留。",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.worker.terminate()
                self.worker.wait()
                self.status_label.setText("索引已停止")
                self.indexing_finished()

    def focus_search(self):
        """聚焦到搜索框并选中所有文本"""
        self.search_input.setFocus()
        self.search_input.selectAll()

def main():
    app = QApplication(sys.argv)
    window = EverythingGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
