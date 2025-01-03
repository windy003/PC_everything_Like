import sys
import os
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLineEdit, QPushButton, QTableWidget, 
                            QTableWidgetItem, QHeaderView, QFileDialog, QMenuBar,
                            QMenu, QMessageBox, QLabel, QProgressBar, QSystemTrayIcon)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
import sqlite3
import win32file
import win32con
import winerror
from PyQt6.QtGui import QKeySequence, QShortcut, QIcon

class FastIndexWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(str, float)

    def __init__(self, drives, db_folder, specific_dir=None):
        super().__init__()
        self.drives = drives
        self.specific_dir = specific_dir
        self.db_folder = db_folder
        print(f"FastIndexWorker 初始化: drives={drives}, specific_dir={specific_dir}, db_folder={db_folder}")

    def run(self):
        print("FastIndexWorker 开始运行")
        start_time = datetime.now()
        temp_db = os.path.join(self.db_folder, 'temp_indexing.db')
        print(f"使用临时数据库: {temp_db}")
        
        try:
            # 创建临时数据库连接
            conn = sqlite3.connect(temp_db)
            
            # 创建表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    filename TEXT,
                    size INTEGER,
                    modified_time TEXT
                )
            """)
            conn.commit()
            
            total_file_count = 0
            batch = []
            
            # 定义要跳过的目录
            skip_dirs = {
                '$Recycle.Bin',
                '$Windows.~BT',
                '$Windows.~WS',
                '$360Section',
                'System Volume Information',
                'Config.Msi',
                'MSOCache',
                'Windows.old'
            }

            for drive in self.drives:
                if not drive.endswith(':\\'):
                    continue
                
                try:
                    # 如果指定了特定目录，只扫描该目录
                    start_path = self.specific_dir if self.specific_dir else drive
                    
                    self.progress.emit(f"正在扫描 {start_path}...")
                    
                    # 使用 os.walk 进行文件系统扫描
                    for root, dirs, files in os.walk(start_path):
                        # 修改 dirs 列表来跳过不需要的目录
                        dirs[:] = [d for d in dirs if d not in skip_dirs and not d.startswith('$')]
                        
                        if self.isInterruptionRequested():
                            raise InterruptedError("索引过程被用户终止")
                        
                        for file in files:
                            try:
                                full_path = os.path.join(root, file)
                                # 跳过隐藏文件和系统文件
                                if os.path.exists(full_path) and not os.path.isdir(full_path):
                                    try:
                                        attrs = win32file.GetFileAttributes(full_path)
                                        is_hidden = attrs & win32file.FILE_ATTRIBUTE_HIDDEN
                                        is_system = attrs & win32file.FILE_ATTRIBUTE_SYSTEM
                                        if not (is_hidden or is_system):
                                            stats = os.stat(full_path)
                                            batch.append((
                                                full_path,
                                                file,
                                                stats.st_size,
                                                datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                                            ))
                                            total_file_count += 1
                                            
                                            if total_file_count % 1000 == 0:
                                                self.progress.emit(f"正在扫描 {drive} - 已找到 {total_file_count} 个文件...")
                                            
                                            if len(batch) >= 10000:
                                                conn.executemany(
                                                    "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
                                                    batch
                                                )
                                                conn.commit()
                                                batch = []
                                    except WindowsError:
                                        continue
                                    
                            except (OSError, FileNotFoundError):
                                continue
                    
                except Exception as e:
                    self.progress.emit(f"处理驱动器 {drive} 时出错: {str(e)}")
                    continue
            
            # 处理剩余的批次
            if batch:
                conn.executemany(
                    "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
                    batch
                )
                conn.commit()
            
            # 计算总耗时
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            
            # 根据扫描类型生成数据库名称
            if self.specific_dir:
                dir_name = os.path.basename(self.specific_dir.rstrip('\\'))
                scan_type = f"Dir_{dir_name}"
            else:
                drives_str = '+'.join(d.replace(':\\', '') for d in self.drives)
                scan_type = f"Drive_{drives_str}"
            
            # 在数据库目录下创建最终数据库文件
            final_db_name = f"{end_time.strftime('%Y-%m-%d_%H-%M-%S')}_{scan_type}.db"
            final_db_path = os.path.join(self.db_folder, final_db_name)
            print(f"最终数据库路径: {final_db_path}")
            
            conn.close()
            
            if os.path.exists(final_db_path):
                os.remove(final_db_path)
            os.rename(temp_db, final_db_path)
            
            self.progress.emit(f"索引完成！共索引 {total_file_count} 个文件")
            self.finished.emit(final_db_path, total_time)
            
        except Exception as e:
            self.progress.emit(f"索引过程出错: {str(e)}")
            if os.path.exists(temp_db):
                os.remove(temp_db)
            self.finished.emit("", 0)

class EverythingGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        # 设置应用图标
        self.icon = QIcon('icon.png')
        self.setWindowIcon(self.icon)
        
        # 创建数据库目录
        self.db_folder = os.path.join(os.path.expanduser('~'), 'Everything_like_By_WZ')
        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)
            print(f"创建数据库目录: {self.db_folder}")
        
        self.db_path = None
        self.conn = None
        self.init_database()
        self.initUI()
        self.init_tray()

    def init_database(self):
        # 尝试从配置文件读取最后使用的数据库路径
        last_db = self.load_last_database()
        if last_db and os.path.exists(os.path.join(self.db_folder, last_db)):
            self.db_path = os.path.join(self.db_folder, last_db)
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()
            print(f"加载上次数据库: {self.db_path}")
        else:
            # 如果没有找到最后使用的数据库，则创建新的
            current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S.db')
            self.db_path = os.path.join(self.db_folder, current_time)
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()
            print(f"创建新数据库: {self.db_path}")
        
        # 保存当前数据库路径
        self.save_last_database()

    def load_last_database(self):
        """加载最后使用的数据库路径"""
        try:
            config_path = os.path.join(self.db_folder, 'config.ini')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    last_db = f.read().strip()
                    print(f"读取到上次数据库路径: {last_db}")
                    return last_db
        except Exception as e:
            print(f"读取配置文件出错: {e}")
        return None

    def save_last_database(self):
        """保存当前数据库路径到配置文件"""
        try:
            config_path = os.path.join(self.db_folder, 'config.ini')
            with open(config_path, 'w', encoding='utf-8') as f:
                # 只保存数据库文件名，不保存完整路径
                db_name = os.path.basename(self.db_path)
                f.write(db_name)
                print(f"保存数据库路径: {db_name}")
        except Exception as e:
            print(f"保存配置文件出错: {e}")

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

        # 搜索区域
        search_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('输入搜索关键词...')
        self.search_input.textChanged.connect(self.search_files)
        
        # 添加快捷键 Alt+D 聚焦到搜索框
        shortcut = QShortcut(QKeySequence("Alt+D"), self)
        shortcut.activated.connect(self.focus_search)

        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)

        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(4)
        self.result_table.setHorizontalHeaderLabels(['文件名', '路径', '大小', '修改时间'])
        
        # 设置表格列的调整方式
        header = self.result_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)
        
        # 设置默认列宽
        self.result_table.setColumnWidth(0, 200)
        self.result_table.setColumnWidth(1, 400)
        self.result_table.setColumnWidth(2, 100)
        self.result_table.setColumnWidth(3, 150)
        
        header.setStretchLastSection(False)
        self.result_table.setShowGrid(True)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setSortingEnabled(True)
        
        layout.addWidget(self.result_table)

        # 创建菜单栏
        menubar = self.menuBar()
        
        # 文件菜单
        file_menu = menubar.addMenu('文件(&F)')
        
        # 添加数据库选择选项
        select_db_action = file_menu.addAction('选择数据库(&O)')
        select_db_action.setShortcut('Ctrl+O')
        select_db_action.triggered.connect(self.select_database)
        
        file_menu.addSeparator()
        
        # 添加退出选项
        exit_action = file_menu.addAction('退出(&Q)')
        exit_action.setShortcut('Alt+F4')
        exit_action.triggered.connect(self.close)
        
        # 索引菜单
        index_menu = menubar.addMenu('索引(&I)')
        
        # 添加索引所有驱动器选项
        index_all_action = index_menu.addAction('索引所有驱动器(&A)')
        index_all_action.setShortcut('Ctrl+A')
        index_all_action.triggered.connect(self.index_all_drives)
        
        # 添加索引特定驱动器选项
        index_drive_action = index_menu.addAction('索引特定驱动器(&D)')
        index_drive_action.setShortcut('Ctrl+D')
        index_drive_action.triggered.connect(self.select_drive_to_index)
        
        # 添加索引特定目录选项
        index_dir_action = index_menu.addAction('索引特定目录(&F)')
        index_dir_action.setShortcut('Ctrl+F')
        index_dir_action.triggered.connect(self.select_directory_to_index)
        
        index_menu.addSeparator()
        
        # 添加停止索引选项
        self.stop_index_action = index_menu.addAction('停止索引(&S)')
        self.stop_index_action.setShortcut('Ctrl+S')
        self.stop_index_action.triggered.connect(self.stop_indexing)
        self.stop_index_action.setEnabled(False)
        
        # 帮助菜单
        help_menu = menubar.addMenu('帮助(&H)')
        
        # 添加关于选项
        about_action = help_menu.addAction('关于(&A)')
        about_action.triggered.connect(self.show_about)

        # 添加状态栏
        self.status_label = QLabel()
        self.statusBar().addWidget(self.status_label)
        
        # 添加进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.hide()
        self.statusBar().addPermanentWidget(self.progress_bar)

        # 更新状态栏显示当前数据库
        self.status_label.setText(f'当前数据库: {self.db_path}')

    def select_database(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self,
            "选择数据库文件",
            self.db_folder,  # 设置默认目录为数据库文件夹
            "SQLite数据库 (*.db);;所有文件 (*.*)"
        )
        if file_name:
            if self.conn:
                self.conn.close()
            self.db_path = file_name
            self.conn = sqlite3.connect(self.db_path)
            self.create_tables()
            self.status_label.setText(f'当前数据库: {os.path.basename(self.db_path)}')
            # 保存当前选择的数据库
            self.save_last_database()

    def create_new_database(self):
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S.db')
        if self.conn:
            self.conn.close()
        self.db_path = current_time
        self.conn = sqlite3.connect(self.db_path)
        self.create_tables()
        self.db_label.setText(f'当前数据库: {self.db_path}')
        # 保存新建的数据库
        self.save_last_database()

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
                
                reply = QMessageBox.question(
                    self,
                    "确认索引",
                    f"确定要索引驱动器 {drive} 吗？\n这可能需要一些时间。",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.Yes:
                    self.index_btn.setEnabled(False)
                    self.stop_index_action.setEnabled(True)
                    self.status_label.setText(f"准备索引驱动器 {drive}...")
                    self.progress_bar.show()
                    
                    # 创建并启动工作线程
                    self.worker = FastIndexWorker(self.db_path, [drive])
                    self.worker.progress.connect(self.update_index_status)
                    self.worker.finished.connect(self.handle_indexing_finished)  # 连接新的处理函数
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
        
        # 更新窗口标题
        self.setWindowTitle('Python Everything - 正在索引...')

    def indexing_finished(self):
        """索引完成后的处理"""
        self.stop_index_action.setEnabled(False)
        self.progress_bar.hide()
        self.setWindowTitle('Python Everything')

    def handle_indexing_finished(self, new_db_path, total_time):
        """处理索引完成并更新数据库路径"""
        if new_db_path:  # 如果索引成功
            # 关闭当前数据库连接
            if self.conn:
                self.conn.close()
            
            # 更新数据库路径和连接
            self.db_path = new_db_path
            self.conn = sqlite3.connect(self.db_path)
            
            # 更新UI显示
            self.status_label.setText(f'当前数据库: {self.db_path}')
            
            # 保存新的数据库路径
            self.save_last_database()
            
            # 格式化时间显示
            hours = int(total_time // 3600)
            minutes = int((total_time % 3600) // 60)
            seconds = int(total_time % 60)
            
            time_str = ""
            if hours > 0:
                time_str += f"{hours}小时"
            if minutes > 0:
                time_str += f"{minutes}分钟"
            if seconds > 0 or not time_str:
                time_str += f"{seconds}秒"
            
            # 显示完成消息，包含耗时信息
            QMessageBox.information(
                self,
                "索引完成",
                f"文件索引已完成！\n"
                f"数据库已保存为: {self.db_path}\n"
                f"总耗时: {time_str}",
                QMessageBox.StandardButton.Ok
            )
        else:  # 如果索引失败
            QMessageBox.warning(
                self,
                "索引失败",
                "文件索引过程中出现错误，请重试。",
                QMessageBox.StandardButton.Ok
            )
        
        self.indexing_finished()

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
        
        reply = QMessageBox.question(
            self,
            '确认',
            f'确定要索引以下驱动器吗？\n{", ".join(drives)}',
            QMessageBox.StandardButton.Yes |
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.stop_index_action.setEnabled(True)
            self.status_label.setText("准备开始索引...")
            self.progress_bar.show()
            
            # 使用 FastIndexWorker
            self.worker = FastIndexWorker(drives, self.db_folder)
            self.worker.progress.connect(self.update_index_status)
            self.worker.finished.connect(self.handle_indexing_finished)
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

    def init_tray(self):
        """初始化系统托盘"""
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(self.icon)
        
        # 创建托盘菜单
        tray_menu = QMenu()
        show_action = tray_menu.addAction("显示窗口")
        show_action.triggered.connect(self.show)
        
        hide_action = tray_menu.addAction("最小化到托盘")
        hide_action.triggered.connect(self.hide)
        
        quit_action = tray_menu.addAction("退出")
        quit_action.triggered.connect(QApplication.instance().quit)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        
        # 双击托盘图标显示窗口
        self.tray_icon.activated.connect(self.tray_icon_activated)

    def tray_icon_activated(self, reason):
        """处理托盘图标的激活事件"""
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            if self.isHidden():
                self.show()
            else:
                self.hide()

    def closeEvent(self, event):
        """重写关闭事件"""
        if self.tray_icon.isVisible():
            QMessageBox.information(
                self,
                "提示",
                '程序将继续在系统托盘运行。要完全退出程序，请右键点击托盘图标选择"退出"。'
            )
            self.hide()
            event.ignore()
        else:
            event.accept()

    def show_about(self):
        """显示关于对话框"""
        QMessageBox.about(
            self,
            "关于 Python Everything",
            "Python Everything\n\n"
            "一个简单的文件索引和搜索工具\n"
            "基于 Python 和 PyQt6 开发\n\n"
            "快捷键：\n"
            "Ctrl+A: 索引所有驱动器\n"
            "Ctrl+S: 停止索引\n"
            "Ctrl+O: 选择数据库\n"
            "Alt+D: 聚焦搜索框"
        )

    def select_drive_to_index(self):
        """选择特定驱动器进行索引"""
        try:
            if os.name == 'nt':  # Windows系统
                import win32api
                drives = win32api.GetLogicalDriveStrings()
                drives = drives.split('\000')[:-1]  # 获取所有驱动器列表
                print(f"可用驱动器: {drives}")  # 调试信息
                
                # 创建驱动器选择对话框
                drive_dialog = QMessageBox(self)
                drive_dialog.setWindowTitle("选择驱动器")
                drive_dialog.setText("请选择要索引的驱动器：")
                
                buttons = []  # 存储按钮引用
                # 为每个驱动器创建按钮
                for drive in drives:
                    btn = drive_dialog.addButton(drive, QMessageBox.ButtonRole.ActionRole)
                    buttons.append(btn)
                
                cancel_btn = drive_dialog.addButton("取消", QMessageBox.ButtonRole.RejectRole)
                
                # 显示对话框
                drive_dialog.exec()
                
                # 获取点击的按钮
                clicked_button = drive_dialog.clickedButton()
                print(f"点击的按钮: {clicked_button.text()}")  # 调试信息
                
                # 如果不是取消按钮
                if clicked_button != cancel_btn:
                    selected_drive = clicked_button.text()
                    print(f"选择的驱动器: {selected_drive}")  # 调试信息
                    
                    # 显示确认对话框
                    reply = QMessageBox.question(
                        self,
                        "确认索引",
                        f"确定要索引驱动器 {selected_drive} 吗？\n这可能需要一些时间。",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                    )
                    
                    if reply == QMessageBox.StandardButton.Yes:
                        print(f"用户确认索引驱动器: {selected_drive}")  # 调试信息
                        self.start_indexing([selected_drive])
                    else:
                        print("用户取消索引")  # 调试信息
                else:
                    print("用户点击了取消按钮")  # 调试信息
                
        except Exception as e:
            print(f"选择驱动器时出错: {str(e)}")  # 调试信息
            QMessageBox.warning(
                self,
                "错误",
                f"选择驱动器时出错: {str(e)}",
                QMessageBox.StandardButton.Ok
            )

    def select_directory_to_index(self):
        """选择特定目录进行索引"""
        directory = QFileDialog.getExistingDirectory(self, "选择要索引的目录")
        if directory:
            # 获取目录所在的驱动器
            drive = os.path.splitdrive(directory)[0] + '\\'
            
            reply = QMessageBox.question(
                self,
                "确认索引",
                f"确定要索引目录 {directory} 吗？\n这可能需要一些时间。",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.start_indexing([drive], specific_dir=directory)

    def start_indexing(self, drives, specific_dir=None):
        """开始索引过程"""
        try:
            print(f"开始索引驱动器: {drives}, 特定目录: {specific_dir}")
            
            self.stop_index_action.setEnabled(True)
            self.status_label.setText("准备开始索引...")
            self.progress_bar.show()
            self.progress_bar.setRange(0, 0)
            
            # 创建并启动工作线程，传递数据库目录
            self.worker = FastIndexWorker(drives, self.db_folder, specific_dir)
            print("已创建 FastIndexWorker")
            
            self.worker.progress.connect(self.update_index_status)
            self.worker.finished.connect(self.handle_indexing_finished)
            print("已连接信号")
            
            self.worker.start()
            print("已启动工作线程")
            
        except Exception as e:
            print(f"启动索引时出错: {str(e)}")
            QMessageBox.warning(
                self,
                "错误",
                f"启动索引过程时出错: {str(e)}",
                QMessageBox.StandardButton.Ok
            )
            self.indexing_finished()

def main():
    app = QApplication(sys.argv)
    # 设置应用程序图标
    app.setWindowIcon(QIcon('icon.png'))  # 确保图片文件存在
    window = EverythingGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
