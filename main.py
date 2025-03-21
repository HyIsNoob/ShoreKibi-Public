import discord
from discord.ext import commands
from discord import app_commands
import os
import json
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError
from datetime import datetime, timedelta
import asyncio
from gtts import gTTS
from config import BOT_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN, YOUTUBE_API_KEY, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
import random
import time
from deep_translator import GoogleTranslator
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

rate_limits = {} 
_dropbox_client = None
FFMPEG_OPTIONS = {
    'before_options': '-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 -probesize 32k -analyzeduration 0',
    'options': '-vn -ar 48000 -ac 2'
}

INVIDIOUS_INSTANCES = [
    "https://invidious.slipfox.xyz",
    "https://invidious.dhusch.de",
    "https://invi.smnz.de",
    "https://y.com.sb",
    "https://invidious.protokolla.fi",
    "https://invidious.private.coffee"
]

PIPED_INSTANCES = [
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.tokhmi.xyz",
    "https://api.piped.projectsegfau.lt",
    "https://pipedapi.syncpundit.io",
    "https://api.piped.privacydev.net"
]

BACKUP_STREAMS = {
    "lofi": "https://stream.0x.no/lofi",
    "jazz": "https://stream.0x.no/jazz",
    "pop": "https://stream.0x.no/pop",
    "rock": "https://ice1.somafm.com/groovesalad-128-mp3",
    "gaming": "https://node-24.zeno.fm/k7b03fcztfhvv?rj-ttl=5&rj-tok=AAABgBaE3m0AHuDG1JZRmrRqCg",
    "trance": "https://stream-36.zeno.fm/8wyuvb0vqv8uv?zs=ciBdKMTuQJK81zEsKI_hgA",
    "kpop": "https://kpop-radio.de/stream.mp3"
}

# Bật intents, bao gồm presences để theo dõi hoạt động game
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.presences = True  
bot = commands.Bot(command_prefix="!", intents=intents)

# Tải custom_data từ file nếu có
if os.path.exists('custom_data.json'):
    try:
        with open('custom_data.json', 'r', encoding='utf-8') as f:
            bot.custom_data = json.load(f)
    except Exception as e:
        print(f"Error loading custom data: {e}")

# Files lưu trữ
WATCHLIST_FILE = "watchlist.json"
SHORE_CHANNEL_FILE = "shore_channel.json"
PLANS_FILE = "plans.json"


def load_json(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# watchlist: {guild_id: [channel_id, ...]}
watchlist = load_json(WATCHLIST_FILE)
shore_channels = load_json(SHORE_CHANNEL_FILE)
plans = load_json(PLANS_FILE)

def save_shore_channel():
    save_json(shore_channels, SHORE_CHANNEL_FILE)

def get_shore_channel(guild_id):
    if guild_id in shore_channels:
        return bot.get_channel(int(shore_channels[guild_id]))
    return None

image_extensions = ['.png', '.jpg', '.jpeg', '.gif']
video_extensions = ['.mp4', '.mov']

async def upload_attachment_to_dropbox(attachment, channel_name):
    """Upload một attachment lên Dropbox và trả về tên file"""
    # Sử dụng ID duy nhất cho mỗi file tạm
    unique_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    temp_filename = f"{attachment.filename.split('.')[0]}_{unique_id}{os.path.splitext(attachment.filename)[1]}"
    local_path = f"temp/{temp_filename}"
    
    # Đảm bảo thư mục tạm tồn tại
    os.makedirs("temp", exist_ok=True)
    
    try:
        # Tải file xuống
        await attachment.save(local_path)
        
        # Lấy client Dropbox đã tạo
        dbx = get_dropbox_client()
        
        # Xác định đường dẫn trên Dropbox
        ext = os.path.splitext(attachment.filename)[1].lower()
        if ext in image_extensions:
            dropbox_path = f"/{channel_name}/images/{attachment.filename}"
        elif ext in video_extensions:
            dropbox_path = f"/{channel_name}/videos/{attachment.filename}"
        else:
            dropbox_path = f"/{channel_name}/{attachment.filename}"
        
        # Kiểm tra file đã tồn tại chưa
        try:
            dbx.files_get_metadata(dropbox_path)
            # File đã tồn tại, không upload nữa
            os.remove(local_path)
            return None
        except ApiError:
            # File chưa tồn tại, tiếp tục upload
            pass
        
        # Upload file lên Dropbox với timeout
        with open(local_path, "rb") as f:
            file_content = f.read()
        
        # Xử lý upload với timeout
        try:
            dbx.files_upload(file_content, dropbox_path, mode=WriteMode("add"))
            return attachment.filename
        except Exception as e:
            print(f"Lỗi khi upload {attachment.filename}: {e}")
            return None
            
    except Exception as e:
        print(f"Lỗi xử lý file {attachment.filename}: {e}")
        return None
    finally:
        # Dọn dẹp file tạm
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except:
                pass

# ---------------- Valorant Custom Functionality ----------------
# Global dictionary lưu dữ liệu giải Valorant theo guild
valorant_data = {}  # key: guild_id, value: dict chứa thông tin đăng ký, thiết lập trận đấu, score, v.v.

class ValorantRegistrationView(discord.ui.View):
    def __init__(self, guild_id: str, match_date: str, match_time: str, creator_id: int):
        super().__init__(timeout=None)  # Changed timeout to None for persistent view
        self.guild_id = guild_id
        self.match_date = match_date
        self.match_time = match_time
        self.creator_id = creator_id  # Store creator ID
        self.team1 = []
        self.team2 = []
        self.team2_guest = False
        self.message = None  # Store the message for embed updates

    async def update_embed(self):
        if not self.message:
            return
        
        # Create updated embed with current team information
        embed = discord.Embed(
            title="Valorant Custom Tournament",
            description=f"Thời gian diễn ra: {self.match_date} {self.match_time}",
            color=0xFD4553  # Valorant red color
        )
        
        # Team 1 status
        if self.team1:
            team1_members = "\n".join(f"<@{uid}>" for uid in self.team1)
            remaining = max(0, 5 - len(self.team1))
            if remaining > 0:
                team1_members += f"\n*Còn cần thêm {remaining} người*"
        else:
            team1_members = "*Chưa có thành viên*"
        embed.add_field(name="Đội 1", value=team1_members, inline=True)
        
        # Team 2 status
        if self.team2_guest:
            team2_members = "*Chế độ Đội khách - Chỉ cần 1 thành viên đại diện*"
            if self.team2:
                team2_members += f"\nĐại diện: <@{self.team2[0]}>"
        else:
            if self.team2:
                team2_members = "\n".join(f"<@{uid}>" for uid in self.team2)
                remaining = max(0, 5 - len(self.team2))
                if remaining > 0:
                    team2_members += f"\n*Còn cần thêm {remaining} người*"
            else:
                team2_members = "*Chưa có thành viên*"
        embed.add_field(name="Đội 2", value=team2_members, inline=True)
        
        # Add footer instructions
        embed.set_footer(text="Bấm các nút phía dưới để đăng ký vào đội")
        
        # Update the embed
        await self.message.edit(embed=embed)

    @discord.ui.button(label="Tham gia Đội 1", style=discord.ButtonStyle.primary, custom_id="valorant:join_team1")
    async def join_team1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        uid = interaction.user.id
        if uid in self.team1:
            await interaction.response.send_message("Cậu đã đăng ký vào Đội 1 rồi!", ephemeral=True)
        elif len(self.team1) >= 5:
            await interaction.response.send_message("Đội 1 đã đủ 5 người!", ephemeral=True)
        else:
            self.team1.append(uid)
            await self.update_embed()

    @discord.ui.button(label="Tham gia Đội 2", style=discord.ButtonStyle.primary, custom_id="valorant:join_team2")
    async def join_team2(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = interaction.user.id
        if self.team2_guest:
            if len(self.team2) >= 1:
                await interaction.response.send_message("Đội 2 đã có người đại diện rồi!", ephemeral=True)
            else:
                self.team2.append(uid)
                await interaction.response.send_message("Đã đăng ký làm người đại diện Đội khách!", ephemeral=True)
                await self.update_embed()
        elif uid in self.team2:
            await interaction.response.send_message("Cậu đã đăng ký vào Đội 2 rồi!", ephemeral=True)
        elif len(self.team2) >= 5:
            await interaction.response.send_message("Đội 2 đã đủ 5 người!", ephemeral=True)
        else:
            self.team2.append(uid)
            await self.update_embed()

    @discord.ui.button(label="Chọn Đội khách", style=discord.ButtonStyle.secondary, custom_id="valorant:guest_team")
    async def guest_team(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        self.team2_guest = True
        self.team2 = []
        await self.update_embed()

    @discord.ui.button(label="Xác nhận đăng ký", style=discord.ButtonStyle.success, custom_id="valorant:confirm_registration")
    async def confirm_registration(self, interaction: discord.Interaction, button: discord.ui.Button):
        if len(self.team1) < 5:
            await interaction.response.send_message("Đội 1 chưa đủ 5 người.", ephemeral=True)
            return
        if not self.team2_guest and len(self.team2) < 5:
            await interaction.response.send_message("Đội 2 chưa đủ 5 người hoặc hãy chọn chế độ Đội khách.", ephemeral=True)
            return
        if self.team2_guest and not self.team2:
            await interaction.response.send_message("Vui lòng chọn ít nhất 1 người đại diện cho Đội khách.", ephemeral=True)
            return
            
        valorant_data[str(interaction.guild_id)] = {
            "match_date": self.match_date,
            "match_time": self.match_time,
            "team1": self.team1,
            "team2": self.team2,
            "guest_mode": self.team2_guest,
            "map": None,
            "side": None,
            "mode": None,
            "score": {"team1": 0, "team2": 0}
        }
        
        # Vô hiệu hóa các nút sau khi đăng ký thành công
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        
        # Tạo view thiết lập trận đấu mới
        match_setup_view = MatchSetupView(str(interaction.guild_id))
        
        # Chỉ bật nút chọn chế độ, vô hiệu hóa các nút khác
        for child in match_setup_view.children:
            child.disabled = True
            if isinstance(child, discord.ui.Select) and child.custom_id == "valorant:select_mode":
                child.disabled = False
        
        # Thêm ephemeral=False để đảm bảo hiển thị cho mọi người
        await interaction.response.send_message("Đăng ký thành công! Vui lòng chọn chế độ đấu (BO1, BO3 hoặc BO5) để thiết lập trận đấu:", view=match_setup_view, ephemeral=False)

    @discord.ui.button(label="Huỷ giải đấu", style=discord.ButtonStyle.danger, custom_id="valorant:cancel_tournament")
    async def cancel_tournament(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the tournament and disable this view"""
        # Check if the user who clicked is the creator
        if interaction.user.id != self.creator_id:
            await interaction.response.send_message("Chỉ người tạo giải đấu mới có thể huỷ giải đấu này.", ephemeral=True)
            return
            
        # Clear the guild's tournament data if it exists
        if self.guild_id in valorant_data:
            del valorant_data[self.guild_id]
            
        # Disable all buttons
        for child in self.children:
            child.disabled = True
            
        # Update the embed to show it's cancelled
        embed = discord.Embed(
            title="Valorant Custom Tournament - ĐÃ HUỶ",
            description=f"Giải đấu dự kiến diễn ra vào {self.match_date} {self.match_time} đã bị huỷ.",
            color=0x808080  # Grey color for cancelled status
        )
        embed.set_footer(text="Giải đấu này đã bị huỷ. Sử dụng /valorantcustom để tạo giải mới.")
        
        await self.message.edit(embed=embed, view=self)
        await interaction.response.send_message("Giải đấu Valorant đã bị huỷ.", ephemeral=True)

# Thêm thuộc tính mới vào MatchSetupView
class MatchSetupView(discord.ui.View):
    def __init__(self, guild_id: str):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.selected_map = None
        self.selected_side = None  # Phe của Đội 1
        self.selected_mode = None  # BO1, BO3, BO5
        self.banned_maps = []  # Danh sách map bị ban
        self.ban_turn = 1  # 1 = Đội 1, 2 = Đội 2
        self.ban_count = {1: 0, 2: 0}  # Đếm số lượng map đã ban của mỗi đội
        self.manual_map_selection = False
        self.channel = None
        self.maps_per_team = {}  # Lưu map do mỗi đội chọn: {1: [map1, map2], 2: [map1, map2]}
        self.map_selection_turn = None  # Lưu lượt chọn map: 1 = Đội 1, 2 = Đội 2
        self.side_selection_turn = None  # Lưu lượt chọn phe: 1 = Đội 1, 2 = Đội 2
        self.first_pick_team = None  # Đội được chọn trước: 1 hoặc 2
        self.current_step = "select_mode"  # Bước hiện tại: "select_mode", "ban_map", "choose_first", "map", "side"
        self.map_side_pairs = []  # Lưu cặp map/phe: [(map1, side1), (map2, side2),...]
        self.current_round = 0  # Round hiện tại (0-based)
        self.last_picked_map = None  # Map vừa được chọn
        self.last_team_picked_map = None  # Đội vừa chọn map
        self.side_chosen_first = False

    @discord.ui.button(label="Đội 1 ban map", style=discord.ButtonStyle.secondary, custom_id="valorant:team1_ban_map")
    async def team1_ban_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 1 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        # Convert user_id để so sánh đúng
        user_id = interaction.user.id  # Số nguyên
        team1_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team1"]]
        
        if user_id not in team1_ids:
            await interaction.response.send_message("Chỉ thành viên Đội 1 mới có thể ban map cho đội mình.", ephemeral=True)
            return
        
        if self.ban_turn != 1 or self.ban_count[1] >= 2:
            await interaction.response.send_message("Không phải lượt ban của Đội 1 hoặc Đội 1 đã ban đủ 2 map.", ephemeral=True)
            return

        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
            
        all_maps = ["Ascent", "Icebox", "Fracture", "Haven", "Lotus", "Pearl", "Split", "Abyss", "Sunset", "Bind", "Breeze"]
        available_maps = [m for m in all_maps if m not in self.banned_maps]
        
        # Create a safer approach for temporary views
        class TeamMapBanView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)  # Shorter timeout for this temporary view
                self.selected_map = None
                
            @discord.ui.select(
                placeholder="Chọn map muốn ban",
                min_values=1,
                max_values=1,
                options=[discord.SelectOption(label=m) for m in available_maps]
            )
            async def select_map(self, select_interaction: discord.Interaction, select: discord.ui.Select):
                self.selected_map = select.values[0]
                self.disable_all_items()
                await select_interaction.response.edit_message(view=self, content=f"Đã chọn ban map: {self.selected_map}")
                self.stop()
                
            def disable_all_items(self):
                for item in self.children:
                    item.disabled = True

        map_ban_view = TeamMapBanView()
        await interaction.response.send_message("Chọn map muốn ban:", view=map_ban_view, ephemeral=True)
        
        # Wait for the view to be completed
        await map_ban_view.wait()
        selected_map = map_ban_view.selected_map
        
        # If the view timed out and no map was selected
        if not selected_map:
            try:
                await interaction.followup.send("Ban map đã hết hạn. Vui lòng thử lại.", ephemeral=True)
            except:
                pass
            return
            
        # Apply the ban
        self.banned_maps.append(selected_map)
        await self.channel.send(f"**Đội 1** đã ban map **{selected_map}**")
        self.ban_count[1] += 1
        self.ban_turn = 2  # Chuyển lượt cho đội 2
        
        # Nếu đã ban đủ 4 map (mỗi đội 2), chuyển sang bước chọn đội đi trước
        if self.ban_count[1] >= 2 and self.ban_count[2] >= 2:
            self.current_step = "choose_first"
            await self.channel.send("Đã ban xong các map. Tiếp theo, các đội cần chọn đội sẽ được chọn map trước.")
            
        # Cập nhật nút
        await self.update_buttons(interaction)
        
    @discord.ui.button(label="Đội 2 ban map", style=discord.ButtonStyle.secondary, custom_id="valorant:team2_ban_map")
    async def team2_ban_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 2 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        user_id = interaction.user.id  # Số nguyên
        
        if data["guest_mode"]:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ đại diện Đội 2 mới có thể ban map cho đội mình.", ephemeral=True)
                return
        else:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ thành viên Đội 2 mới có thể ban map cho đội mình.", ephemeral=True)
                return
        
        if self.ban_turn != 2 or self.ban_count[2] >= 2:
            await interaction.response.send_message("Không phải lượt ban của Đội 2 hoặc Đội 2 đã ban đủ 2 map.", ephemeral=True)
            return

        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
            
        all_maps = ["Ascent", "Icebox", "Fracture", "Haven", "Lotus", "Pearl", "Split", "Abyss", "Sunset", "Bind", "Breeze"]
        available_maps = [m for m in all_maps if m not in self.banned_maps]
        
        # Create a safer approach for temporary views
        class TeamMapBanView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)  # Shorter timeout for this temporary view
                self.selected_map = None
                
            @discord.ui.select(
                placeholder="Chọn map muốn ban",
                min_values=1,
                max_values=1,
                options=[discord.SelectOption(label=m) for m in available_maps]
            )
            async def select_map(self, select_interaction: discord.Interaction, select: discord.ui.Select):
                self.selected_map = select.values[0]
                self.disable_all_items()
                await select_interaction.response.edit_message(view=self, content=f"Đã chọn ban map: {self.selected_map}")
                self.stop()
                
            def disable_all_items(self):
                for item in self.children:
                    item.disabled = True

        map_ban_view = TeamMapBanView()
        await interaction.response.send_message("Chọn map muốn ban:", view=map_ban_view, ephemeral=True)
        
        # Wait for the view to be completed
        await map_ban_view.wait()
        selected_map = map_ban_view.selected_map
        
        # If the view timed out and no map was selected
        if not selected_map:
            try:
                await interaction.followup.send("Ban map đã hết hạn. Vui lòng thử lại.", ephemeral=True)
            except:
                pass
            return
            
        # Apply the ban
        self.banned_maps.append(selected_map)
        await self.channel.send(f"**Đội 2** đã ban map **{selected_map}**\n")
        self.ban_count[2] += 1
        self.ban_turn = 1  # Chuyển lượt cho đội 1
        
        # Nếu đã ban đủ 4 map (mỗi đội 2), chuyển sang bước chọn đội đi trước
        if self.ban_count[1] >= 2 and self.ban_count[2] >= 2:
            self.current_step = "choose_first"
            await self.channel.send("Đã ban xong các map. Tiếp theo, các đội cần chọn đội sẽ được chọn map trước.")
        
        # Cập nhật nút
        await self.update_buttons(interaction)
    
    @discord.ui.button(label="Đội 1 chọn map", style=discord.ButtonStyle.primary, custom_id="valorant:team1_choose_map")
    async def team1_choose_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 1 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        user_id = interaction.user.id
        team1_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team1"]]
        
        if user_id not in team1_ids:
            await interaction.response.send_message("Chỉ thành viên Đội 1 mới có thể chọn map cho đội mình.", ephemeral=True)
            return
        
        # Kiểm tra thứ tự chọn đã được thiết lập chưa
        if self.first_pick_team is None:
            await interaction.response.send_message("Vui lòng chọn đội được chọn trước.", ephemeral=True)
            return
        
        # Kiểm tra có phải lượt chọn map của Đội 1 không
        if self.map_selection_turn != 1:
            await interaction.response.send_message("Hiện không phải lượt chọn map của Đội 1.", ephemeral=True)
            return
        
        # Kiểm tra chế độ BO1
        if self.selected_mode == "BO1" and self.side_chosen_first and self.map_selection_turn != 1:
            await interaction.response.send_message("Trong chế độ BO1, vì Đội 1 đã chọn phe trước nên Đội 2 sẽ được chọn map.", ephemeral=True)
            return
        
        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
        
        # Loại bỏ các map đã bị ban và map đã được chọn
        all_maps = ["Ascent", "Icebox", "Fracture", "Haven", "Lotus", "Pearl", "Split", "Abyss", "Sunset", "Bind", "Breeze"]
        selected_maps = []
        for maps in self.maps_per_team.values():
            selected_maps.extend(maps)
        available_maps = [m for m in all_maps if m not in self.banned_maps and m not in selected_maps]
        
        if not available_maps:
            await interaction.response.send_message("Không còn map nào để chọn!", ephemeral=True)
            return
        
        # Create select menu for map selection
        class MapSelectionView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)
                self.selected_map = None
                
            @discord.ui.select(
                placeholder="Chọn map cho trận đấu",
                min_values=1,
                max_values=1,
                options=[discord.SelectOption(label=m) for m in available_maps]
            )
            async def select_map(self, select_interaction: discord.Interaction, select: discord.ui.Select):
                self.selected_map = select.values[0]
                self.disable_all_items()
                await select_interaction.response.edit_message(view=self, content=f"Đã chọn map: {self.selected_map}")
                self.stop()
                
            def disable_all_items(self):
                for item in self.children:
                    item.disabled = True
        
        map_selection_view = MapSelectionView()
        await interaction.response.send_message("Chọn map cho Đội 1:", view=map_selection_view, ephemeral=True)
        
        # Wait for selection
        await map_selection_view.wait()
        selected_map = map_selection_view.selected_map
        
        if not selected_map:
            try:
                await interaction.followup.send("Chọn map đã hết hạn. Vui lòng thử lại.", ephemeral=True)
            except:
                pass
            return
        
        # Lưu map đã chọn
        if 1 not in self.maps_per_team:
            self.maps_per_team[1] = []
        self.maps_per_team[1].append(selected_map)
        
        # Lưu map vừa chọn và đội đã chọn
        self.last_picked_map = selected_map
        self.last_team_picked_map = 1
        
        self.map_selection_turn = None
        self.current_step = "side"

        # Luôn đặt side_selection_turn = 2 sau khi đội 1 chọn map
        # Bất kể đội nào chọn trước, đội khác luôn chọn phe
        self.side_selection_turn = 2  # Đội 2 luôn chọn phe sau khi đội 1 chọn map

        # Cập nhật nút
        await self.update_buttons(interaction)
        
        # Thông báo
        await self.channel.send(f"**Đội 1** đã chọn map **{selected_map}**. Tiếp theo, Đội {self.side_selection_turn} sẽ chọn phe.")

    @discord.ui.button(label="Đội 2 chọn map", style=discord.ButtonStyle.primary, custom_id="valorant:team2_choose_map")
    async def team2_choose_map(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 2 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        user_id = interaction.user.id
        
        if data["guest_mode"]:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ đại diện Đội 2 mới có thể chọn map cho đội mình.", ephemeral=True)
                return
        else:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ thành viên Đội 2 mới có thể chọn map cho đội mình.", ephemeral=True)
                return
        
        # Kiểm tra lượt chọn map
        if self.map_selection_turn and self.map_selection_turn != 2:
            await interaction.response.send_message("Hiện không phải lượt chọn map của Đội 2.", ephemeral=True)
            return
        
        # Kiểm tra chế độ BO1
        if self.selected_mode == "BO1" and self.side_chosen_first and self.map_selection_turn != 1:
            await interaction.response.send_message("Trong chế độ BO1, vì Đội 1 đã chọn phe trước nên Đội 2 sẽ được chọn map.", ephemeral=True)
            return
        
        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
        
        # Loại bỏ các map đã bị ban và map đã được chọn
        all_maps = ["Ascent", "Icebox", "Fracture", "Haven", "Lotus", "Pearl", "Split", "Abyss", "Sunset", "Bind", "Breeze"]
        selected_maps = []
        for maps in self.maps_per_team.values():
            selected_maps.extend(maps)
        available_maps = [m for m in all_maps if m not in self.banned_maps and m not in selected_maps]
        
        if not available_maps:
            await interaction.response.send_message("Không còn map nào để chọn!", ephemeral=True)
            return
        
        # Create select menu for map selection
        class MapSelectionView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)
                self.selected_map = None
                
            @discord.ui.select(
                placeholder="Chọn map cho trận đấu",
                min_values=1,
                max_values=1,
                options=[discord.SelectOption(label=m) for m in available_maps]
            )
            async def select_map(self, select_interaction: discord.Interaction, select: discord.ui.Select):
                self.selected_map = select.values[0]
                self.disable_all_items()
                await select_interaction.response.edit_message(view=self, content=f"Đã chọn map: {self.selected_map}")
                self.stop()
                
            def disable_all_items(self):
                for item in self.children:
                    item.disabled = True
        
        map_selection_view = MapSelectionView()
        await interaction.response.send_message("Chọn map cho Đội 2:", view=map_selection_view, ephemeral=True)
        
        # Wait for selection
        await map_selection_view.wait()
        selected_map = map_selection_view.selected_map
        
        if not selected_map:
            try:
                await interaction.followup.send("Chọn map đã hết hạn. Vui lòng thử lại.", ephemeral=True)
            except:
                pass
            return
        
        # Lưu map đã chọn
        if 2 not in self.maps_per_team:
            self.maps_per_team[2] = []
        self.maps_per_team[2].append(selected_map)
        
        # Lưu map vừa chọn và đội đã chọn
        self.last_picked_map = selected_map
        self.last_team_picked_map = 2
        
        # Chuyển sang bước chọn phe
        self.map_selection_turn = None
        self.current_step = "side"
        
        # Luôn đặt side_selection_turn = 1 sau khi đội 2 chọn map
        # Bất kể đội nào chọn trước, đội khác luôn chọn phe
        self.side_selection_turn = 1  # Đội 1 luôn chọn phe sau khi đội 2 chọn map

        # Cập nhật nút
        await self.update_buttons(interaction)

        # Thông báo chi tiết hơn
        await self.channel.send(f"**Đội 2** đã chọn map **{selected_map}**. Tiếp theo, Đội 1 sẽ chọn phe.")
        
    # Sửa lại select_side để phù hợp với luồng mới
    @discord.ui.select(
        placeholder="Chọn phe", 
        min_values=1, 
        max_values=1,
        options=[
            discord.SelectOption(label="Attacker", description="Tấn công"),
            discord.SelectOption(label="Defender", description="Phòng thủ")
        ],
        custom_id="valorant:select_side"
    )
    async def select_side(self, interaction: discord.Interaction, select: discord.ui.Select):
        await interaction.response.defer(ephemeral=True, thinking=False)
        # Kiểm tra người dùng có quyền chọn phe không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        user_id = interaction.user.id
        team1_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team1"]]
        team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
        
        # Kiểm tra người dùng có thuộc đội đang được chọn phe không
        if self.side_selection_turn == 1 and user_id not in team1_ids:
            await interaction.response.send_message("Chỉ thành viên Đội 1 mới có thể chọn phe cho đội mình.", ephemeral=True)
            return
        elif self.side_selection_turn == 2:
            if data["guest_mode"]:
                if user_id not in team2_ids:
                    await interaction.response.send_message("Chỉ đại diện Đội 2 mới có thể chọn phe cho đội mình.", ephemeral=True)
                    return
            elif user_id not in team2_ids:
                await interaction.response.send_message("Chỉ thành viên Đội 2 mới có thể chọn phe cho đội mình.", ephemeral=True)
                return
        
        # Lưu phe được chọn
        selected_side = select.values[0]
        
        # Lưu cặp map/phe - Sửa lỗi để không phụ thuộc vào map_selection_turn
        if self.last_picked_map is not None:
            current_map = self.last_picked_map
            
            if self.side_selection_turn == 1:
                self.map_side_pairs.append((current_map, {"team1": selected_side, "team2": "Defender" if selected_side == "Attacker" else "Attacker"}))
                side_message = f"**Đội 1** chọn phe **{selected_side}** (Đội 2: **{'Defender' if selected_side == 'Attacker' else 'Attacker'}**)"
            else:
                self.map_side_pairs.append((current_map, {"team1": "Defender" if selected_side == "Attacker" else "Attacker", "team2": selected_side}))
                side_message = f"**Đội 2** chọn phe **{selected_side}** (Đội 1: **{'Defender' if selected_side == 'Attacker' else 'Attacker'}**)"
        else:
            await interaction.response.send_message("Lỗi: Không có map nào được chọn trước đó.", ephemeral=True)
            return
        
        await self.channel.send(side_message)
        
        # Xác định bước tiếp theo dựa trên chế độ đấu
        map_count_required = {"BO1": 1, "BO3": 2, "BO5": 4}.get(self.selected_mode, 1)
        
        # Nếu đã đủ số map cần thiết, chuyển sang bước xác nhận trận đấu
        if len(self.map_side_pairs) >= map_count_required:
            self.current_step = "confirm"
            await self.channel.send("Đã thiết lập đủ map và phe. Vui lòng bấm 'Xác nhận trận đấu' để bắt đầu.")
        else:
            # Đổi lượt chọn map cho round tiếp theo
            self.current_step = "map"
            self.side_selection_turn = None
            if self.first_pick_team == 1:
                self.map_selection_turn = 2  # Đội 2 chọn map tiếp theo
            else:
                self.map_selection_turn = 1  # Đội 1 chọn map tiếp theo
            
            await self.channel.send(f"Tiếp theo, Đội {self.map_selection_turn} sẽ chọn map.")
    
        # Cập nhật nút
        await self.update_buttons(interaction)

# 1. Select mode
    @discord.ui.select(
        placeholder="Chọn chế độ trận đấu", 
        min_values=1, 
        max_values=1,
        options=[
            discord.SelectOption(label="BO1", description="Best of 1"),
            discord.SelectOption(label="BO3", description="Best of 3"),
            discord.SelectOption(label="BO5", description="Best of 5")
        ],
        custom_id="valorant:select_mode"
    )
    async def select_mode(self, interaction: discord.Interaction, select: discord.ui.Select):
        await interaction.response.defer(ephemeral=True, thinking=False)
        self.selected_mode = select.values[0]
        self.current_step = "ban_map"  # Chuyển sang bước ban map
        
        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
            
        # Public announcement
        await self.channel.send(f"**Chế độ trận đấu:** {self.selected_mode}\nCác đội có thể bắt đầu ban map.")
        
        # Cập nhật nút dựa trên trạng thái mới
        await self.update_buttons(interaction)

    # Thêm phương thức mới để cập nhật trạng thái các nút
    async def update_buttons(self, interaction):
        # Vô hiệu hóa tất cả các nút trước
        for child in self.children:
            child.disabled = True
        
        # Bật các nút tương ứng với trạng thái hiện tại
        if self.current_step == "select_mode":
            # Chỉ cho phép chọn chế độ
            for child in self.children:
                if isinstance(child, discord.ui.Select) and child.custom_id == "valorant:select_mode":
                    child.disabled = False
        
        elif self.current_step == "ban_map":
            # Cho phép ban map
            for child in self.children:
                if child.custom_id in ["valorant:team1_ban_map", "valorant:team2_ban_map"]:
                    child.disabled = False
                # Nếu đã ban đủ 4 map (mỗi đội 2), chuyển sang bước chọn đội đi trước
                if self.ban_count[1] >= 2 and self.ban_count[2] >= 2:
                    self.current_step = "choose_first"
                    for child in self.children:
                        if child.custom_id in ["valorant:team1_choose_first", "valorant:team2_choose_first"]:
                            child.disabled = False
        
        elif self.current_step == "choose_first":
            # Cho phép chọn đội đi trước
            for child in self.children:
                if child.custom_id in ["valorant:team1_choose_first", "valorant:team2_choose_first"]:
                    child.disabled = False
        
        elif self.current_step == "map":
            # Cho phép chọn map
            team_to_pick = 1 if self.map_selection_turn == 1 else 2
            for child in self.children:
                if child.custom_id == f"valorant:team{team_to_pick}_choose_map":
                    child.disabled = False
        
        elif self.current_step == "side":
            # Cho phép chọn phe
            for child in self.children:
                if isinstance(child, discord.ui.Select) and child.custom_id == "valorant:select_side":
                    child.disabled = False
        
        elif self.current_step == "confirm":
            # Cho phép xác nhận trận đấu
            for child in self.children:
                if child.custom_id == "valorant:confirm_match":
                    child.disabled = False
        
        # Cập nhật view
        try:
            await interaction.message.edit(view=self)
        except:
            pass
    
    @discord.ui.button(label="Đội 1 chọn trước", style=discord.ButtonStyle.secondary, custom_id="valorant:team1_choose_first", row=4)
    async def team1_choose_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 1 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        user_id = interaction.user.id
        team1_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team1"]]
        
        if user_id not in team1_ids:
            await interaction.response.send_message("Chỉ thành viên Đội 1 mới có thể chọn cho đội mình.", ephemeral=True)
            return
        
        # Kiểm tra chế độ đã được chọn chưa
        if not self.selected_mode:
            await interaction.response.send_message("Vui lòng chọn chế độ trận đấu trước.", ephemeral=True)
            return
        
        # Thiết lập thứ tự chọn
        self.first_pick_team = 1
        self.current_step = "map"  # Bước tiếp theo là chọn map
        self.map_selection_turn = 1  # Đội 1 chọn map đầu tiên
        
        await self.update_buttons(interaction)
        
        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
        
        await self.channel.send("**Đội 1** được chọn trước. Đội 1 sẽ chọn map đầu tiên, Đội 2 sẽ chọn phe.")

    @discord.ui.button(label="Đội 2 chọn trước", style=discord.ButtonStyle.secondary, custom_id="valorant:team2_choose_first", row=4)
    async def team2_choose_first(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Kiểm tra người dùng có trong đội 2 không
        guild_id = str(interaction.guild_id)
        data = valorant_data.get(guild_id)
        if data is None:
            await interaction.response.send_message("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        user_id = interaction.user.id
        
        if data["guest_mode"]:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ đại diện Đội 2 mới có thể chọn cho đội mình.", ephemeral=True)
                return
        else:
            team2_ids = [int(uid) if isinstance(uid, str) else uid for uid in data["team2"]]
            if user_id not in team2_ids:
                await interaction.response.send_message("Chỉ thành viên Đội 2 mới có thể chọn cho đội mình.", ephemeral=True)
                return
        
        # Kiểm tra chế độ đã được chọn chưa
        if not self.selected_mode:
            await interaction.response.send_message("Vui lòng chọn chế độ trận đấu trước.", ephemeral=True)
            return
        
        # Thiết lập thứ tự chọn
        self.first_pick_team = 2
        self.current_step = "map"  # Bước tiếp theo là chọn map
        self.map_selection_turn = 2  # Đội 2 chọn map đầu tiên
        
        await self.update_buttons(interaction)
        
        # Store the channel for public announcements if not already stored
        if not self.channel:
            self.channel = interaction.channel
        
        await self.channel.send("**Đội 2** được chọn trước. Đội 2 sẽ chọn map đầu tiên, Đội 1 sẽ chọn phe.")
    
    @discord.ui.button(label="Xác nhận trận đấu", style=discord.ButtonStyle.success, custom_id="valorant:confirm_match", row=4)
    async def confirm_match(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Thêm defer() trước khi xử lý
        await interaction.response.defer(ephemeral=False)
        
        data = valorant_data.get(str(interaction.guild_id))
        if data is None:
            await interaction.followup.send("Chưa có đăng ký giải Valorant.", ephemeral=True)
            return
        
        # Kiểm tra các trường thông tin cần thiết
        if not self.selected_mode:
            await interaction.followup.send("Vui lòng chọn chế độ trận đấu.", ephemeral=True)
            return
        
        if not self.map_side_pairs:
            await interaction.followup.send("Vui lòng thiết lập map và phe cho trận đấu.", ephemeral=True)
            return
        
        # Cập nhật dữ liệu
        data["mode"] = self.selected_mode
        data["map_side_pairs"] = self.map_side_pairs
        data["current_round"] = 1
        data["current_pair_index"] = 0
        
        # Thiết lập map và phe đầu tiên
        current_pair = self.map_side_pairs[0]
        data["current_map"] = current_pair[0]
        data["current_sides"] = current_pair[1]
        
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        
        # Hiển thị embed trận đấu
        embed = discord.Embed(
            title="Trận đấu Valorant",
            description=f"Ngày: {data['match_date']}\nChế độ: {data['mode']}",
            color=0x3498db, 
            timestamp=datetime.now()
        )

        # Hiển thị thông tin map hiện tại
        embed.add_field(
            name="Map hiện tại", 
            value=f"{data['current_map']} (Round {data['current_round']})", 
            inline=False
        )

        # Hiển thị thông tin đội
        team1_str = ", ".join(f"<@{uid}>" for uid in data["team1"])
        if data["guest_mode"]:
            team2_str = "Đội khách"
        else:
            team2_str = ", ".join(f"<@{uid}>" for uid in data["team2"])

        embed.add_field(
            name=f"Đội 1 ({data['current_sides']['team1']})", 
            value=team1_str, 
            inline=True
        )
        embed.add_field(
            name=f"Đội 2 ({data['current_sides']['team2']})", 
            value=team2_str, 
            inline=True
        )
        
        # Hiển thị tỉ số
        embed.add_field(
            name="Tỉ số", 
            value=f"**{data['score']['team1']} - {data['score']['team2']}**", 
            inline=False
        )
        
        # Hiển thị thứ tự map
        map_order_str = "\n".join([f"{i+1}. {pair[0]} (Đội 1: {pair[1]['team1']})" for i, pair in enumerate(self.map_side_pairs)])
        embed.add_field(name="Thứ tự map", value=map_order_str, inline=False)
        
        view = ScoreView(str(interaction.guild_id), data["mode"])
        await interaction.followup.send(embed=embed, view=view)
        
class ScoreView(discord.ui.View):
    def __init__(self, guild_id: str, mode: str):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.mode = mode
        self.threshold = {"BO1": 1, "BO3": 2, "BO5": 3}.get(mode, 1)
        self.message = None  # Lưu message để cập nhật embed
        
        # Ẩn nút "Bắt đầu hiệp cuối cùng" ban đầu
        for child in self.children:
            if child.custom_id == "valorant:final_round":
                child.disabled = True

    # Thêm vào ScoreView
    @discord.ui.button(label="Team 1 thắng", style=discord.ButtonStyle.primary, custom_id="valorant:team1_win")
    async def team1_win(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = valorant_data.get(self.guild_id)
        if not data:
            await interaction.response.send_message("Không có dữ liệu trận đấu.", ephemeral=True)
            return
        
        # Kiểm tra người dùng có quyền không
        is_admin = interaction.permissions.administrator
        in_team1 = str(interaction.user.id) in data["team1"]
        in_team2 = data["guest_mode"] and str(interaction.user.id) in data["team2"] or not data["guest_mode"] and str(interaction.user.id) in data["team2"]
        
        if not (is_admin or in_team1 or in_team2):
            await interaction.response.send_message("Bạn không có quyền cập nhật tỉ số.", ephemeral=True)
            return
        
        # Hiển thị view xác nhận
        view = ConfirmScoreView("team1")
        await interaction.response.send_message(f"Xác nhận Đội 1 thắng round {data['current_round']}?", view=view, ephemeral=True)
        
        # Đợi xác nhận
        await view.wait()
        if view.value:
            # Cập nhật tỉ số
            data["score"]["team1"] += 1
            
            # Lưu message nếu chưa có
            if not self.message and hasattr(interaction, 'message'):
                self.message = interaction.message
            
            # Cập nhật trạng thái trận đấu
            await self.update_match_state(interaction, winner="team1")
                    
    @discord.ui.button(label="Team 2 thắng", style=discord.ButtonStyle.primary, custom_id="valorant:team2_win")
    async def team2_win(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = valorant_data.get(self.guild_id)
        if not data:
            await interaction.response.send_message("Không có dữ liệu trận đấu.", ephemeral=True)
            return
        
        # Kiểm tra người dùng có quyền không
        is_admin = interaction.permissions.administrator
        in_team1 = str(interaction.user.id) in data["team1"]
        in_team2 = data["guest_mode"] and str(interaction.user.id) in data["team2"] or not data["guest_mode"] and str(interaction.user.id) in data["team2"]
        
        if not (is_admin or in_team1 or in_team2):
            await interaction.response.send_message("Bạn không có quyền cập nhật tỉ số.", ephemeral=True)
            return
        
        # Hiển thị view xác nhận
        view = ConfirmScoreView("team2")
        await interaction.response.send_message(f"Xác nhận Đội 2 thắng round {data['current_round']}?", view=view, ephemeral=True)
        
        # Đợi xác nhận
        await view.wait()
        if view.value:
            # Cập nhật tỉ số
            data["score"]["team2"] += 1
            
            # Lưu message nếu chưa có
            if not self.message and hasattr(interaction, 'message'):
                self.message = interaction.message
                
            # Cập nhật trạng thái trận đấu
            await self.update_match_state(interaction, winner="team2")
    
    async def update_match_state(self, interaction, winner):
        data = valorant_data.get(self.guild_id)
        
        # Kiểm tra có phải kết thúc trận đấu chưa
        team1_score = data["score"]["team1"]
        team2_score = data["score"]["team2"]
        
        # Check if this is a tiebreaker situation
        is_tiebreaker_ready = (self.mode == "BO3" and team1_score == 1 and team2_score == 1) or \
                            (self.mode == "BO5" and team1_score == 2 and team2_score == 2)
        
        # Enable final round button if needed
        if is_tiebreaker_ready:
            for child in self.children:
                if child.custom_id == "valorant:final_round":
                    child.disabled = False
        
        # Kiểm tra chiến thắng chung cuộc
        match_ended = False
        winner_team = None
        winner_number = None
        
        if team1_score >= self.threshold:
            match_ended = True
            winner_team = "Đội 1"
            winner_number = 1
        elif team2_score >= self.threshold:
            match_ended = True
            winner_team = "Đội 2" 
            winner_number = 2
        
        if match_ended:
            # Tạo embed thông báo chiến thắng đặc biệt
            embed = discord.Embed(
                title=f"🏆 {winner_team} ĐÃ CHIẾN THẮNG! 🏆",
                description=f"**{winner_team}** đã chiến thắng trận đấu với tỷ số **{team1_score} - {team2_score}**!",
                color=0xFFD700  # Màu vàng kim cho nhà vô địch
            )
            
            # Thêm thông tin thành viên đội thắng
            winning_team_members = ", ".join(f"<@{uid}>" for uid in data[f"team{winner_number}"])
            embed.add_field(
                name="👑 Nhà vô địch 👑", 
                value=winning_team_members if winner_number == 1 or not data["guest_mode"] else "Đội khách",
                inline=False
            )
            
            # Hiển thị lịch sử các map đã chơi
            map_history = ""
            for i, pair in enumerate(data["map_side_pairs"]):
                map_history += f"Map {i+1}: **{pair[0]}** (Đội 1: {pair[1]['team1']}, Đội 2: {pair[1]['team2']})\n"
            
            embed.add_field(
                name="🗺️ Maps đã thi đấu", 
                value=map_history, 
                inline=False
            )
            
            # Thêm hình ảnh chiến thắng
            if winner_number == 1:
                embed.set_thumbnail(url="https://i.imgur.com/JyoYEpO.png")  # Team 1 trophy
            else:
                embed.set_thumbnail(url="https://i.imgur.com/FwVhVlZ.png")  # Team 2 trophy
                
            embed.set_footer(text=f"Valorant Custom Tournament • {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            
            # Gửi thông báo đặc biệt
            await interaction.followup.send(
                f"🎮 **TRẬN ĐẤU KẾT THÚC!** 🎮\n" + 
                f"🎉 **{winner_team}** đã chiến thắng! 🎊", 
                embed=embed, 
                ephemeral=False
            )
            
            self.disable_buttons()
            if self.message:
                await self.message.edit(view=self)
        else:
            # Nếu chưa kết thúc, chuẩn bị cho round tiếp theo
            # Tăng round
            data["current_round"] += 1
            
            # Kiểm tra có cặp map/phe tiếp theo không
            if "map_side_pairs" in data and len(data["map_side_pairs"]) > data["current_pair_index"] + 1:
                data["current_pair_index"] += 1
                current_pair = data["map_side_pairs"][data["current_pair_index"]]
                data["current_map"] = current_pair[0]
                data["current_sides"] = current_pair[1]
            else:
                # Nếu không có cặp tiếp theo, đổi phe trên map hiện tại
                data["current_sides"]["team1"] = "Defender" if data["current_sides"]["team1"] == "Attacker" else "Attacker"
                data["current_sides"]["team2"] = "Defender" if data["current_sides"]["team2"] == "Attacker" else "Attacker"
        
        # Cập nhật embed
        if self.message:
            embed = self.message.embeds[0]
            await self.message.edit(embed=embed, view=self)
            # Cập nhật map hiện tại
            for i, field in enumerate(embed.fields):
                if field.name == "Map hiện tại":
                    embed.set_field_at(
                        i, 
                        name="Map hiện tại", 
                        value=f"{data['current_map']} (Round {data['current_round']})",
                        inline=False
                    )
                elif field.name.startswith("Đội 1"):
                    embed.set_field_at(
                        i,
                        name=f"Đội 1 ({data['current_sides']['team1']})",
                        value=field.value,
                        inline=True
                    )
                elif field.name.startswith("Đội 2"):
                    embed.set_field_at(
                        i,
                        name=f"Đội 2 ({data['current_sides']['team2']})",
                        value=field.value,
                        inline=True
                    )
                elif field.name == "Tỉ số":
                    embed.set_field_at(
                        i,
                        name="Tỉ số",
                        value=f"**{team1_score} - {team2_score}**",
                        inline=False
                    )
            
            # Nếu trận đấu kết thúc, cập nhật tiêu đề
            if match_ended:
                embed.title = f"Trận đấu Valorant - {winner_team} Chiến Thắng!"
                embed.color = discord.Color.green()
            
            await self.message.edit(embed=embed, view=self)
            
    @discord.ui.button(label="Bắt đầu hiệp cuối cùng", style=discord.ButtonStyle.danger, custom_id="valorant:final_round")
    async def start_final_round(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        data = valorant_data.get(self.guild_id)
        if not data:
            await interaction.response.send_message("Không có dữ liệu trận đấu.", ephemeral=True)
            return
        
        # Kiểm tra xem có phải đang ở trạng thái cần hiệp cuối cùng hay không
        team1_score = data["score"]["team1"]
        team2_score = data["score"]["team2"]
        
        if (self.mode == "BO3" and team1_score == 1 and team2_score == 1) or \
        (self.mode == "BO5" and team1_score == 2 and team2_score == 2):
            # Chọn map ngẫu nhiên từ các map chưa được chơi hoặc banned
            all_maps = ["Ascent", "Icebox", "Fracture", "Haven", "Lotus", "Pearl", "Split", "Abyss", "Sunset", "Bind", "Breeze"]
            played_maps = [pair[0] for pair in data.get("map_side_pairs", [])]
            banned_maps = data.get("banned_maps", [])
            available_maps = [m for m in all_maps if m not in played_maps and m not in banned_maps]
            
            if not available_maps:
                # Nếu tất cả map đã được chơi, chọn từ tất cả map trừ map cuối
                last_map = played_maps[-1] if played_maps else None
                available_maps = [m for m in all_maps if m != last_map]
            
            # Chọn map ngẫu nhiên và phe ngẫu nhiên
            final_map = random.choice(available_maps)
            sides = ["Attacker", "Defender"]
            random.shuffle(sides)
            team1_side, team2_side = sides
            
            # Cập nhật thông tin trận đấu
            final_pair = (final_map, {"team1": team1_side, "team2": team2_side})
            if "map_side_pairs" not in data:
                data["map_side_pairs"] = []
            data["map_side_pairs"].append(final_pair)
            
            # Thiết lập round mới
            data["current_round"] = max(team1_score, team2_score) + 1
            data["current_pair_index"] = len(data["map_side_pairs"]) - 1
            data["current_map"] = final_map
            data["current_sides"] = {"team1": team1_side, "team2": team2_side}
            
            # Cập nhật embed
            if self.message:
                embed = self.message.embeds[0]
                
                # Cập nhật map hiện tại
                for i, field in enumerate(embed.fields):
                    if field.name == "Map hiện tại":
                        embed.set_field_at(
                            i, 
                            name="Map hiện tại", 
                            value=f"{final_map} (Round {data['current_round']} - HIỆP CUỐI CÙNG)",
                            inline=False
                        )
                    elif field.name.startswith("Đội 1"):
                        embed.set_field_at(
                            i,
                            name=f"Đội 1 ({team1_side})",
                            value=field.value,
                            inline=True
                        )
                    elif field.name.startswith("Đội 2"):
                        embed.set_field_at(
                            i,
                            name=f"Đội 2 ({team2_side})",
                            value=field.value,
                            inline=True
                        )
                
                # Cập nhật thứ tự map
                map_order_str = "\n".join([f"{i+1}. {pair[0]}" for i, pair in enumerate(data["map_side_pairs"])])
                for i, field in enumerate(embed.fields):
                    if field.name == "Thứ tự map":
                        embed.set_field_at(
                            i,
                            name="Thứ tự map",
                            value=map_order_str + f"\n**HIỆP CUỐI: {final_map}** (Đội 1: {team1_side}, Đội 2: {team2_side})",
                            inline=False
                        )
                
                # Vô hiệu hóa nút hiệp cuối cùng
                for child in self.children:
                    if child.custom_id == "valorant:final_round":
                        child.disabled = True
                
                await self.message.edit(embed=embed, view=self)
            
            # Thông báo
            await interaction.response.send_message(
                f"**HIỆP CUỐI CÙNG**\n"
                f"Map: **{final_map}**\n"
                f"Đội 1: **{team1_side}**\n"
                f"Đội 2: **{team2_side}**", 
                ephemeral=False
            )
        else:
            await interaction.response.send_message("Hiện tại chưa cần hiệp cuối cùng.", ephemeral=True)
    
    def disable_buttons(self):
        for child in self.children:
            child.disabled = True
            
class ConfirmScoreView(discord.ui.View):
    def __init__(self, team):
        super().__init__(timeout=30)
        self.value = False
        self.team = team
            
    @discord.ui.button(label="Xác nhận", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        await interaction.response.defer(ephemeral=True, thinking=False)
        
    @discord.ui.button(label="Huỷ", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await interaction.response.send_message("Đã huỷ cập nhật tỉ số.", ephemeral=True)
        
class ConfirmNewTournamentView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)  # Use a shorter timeout for confirmation views
        self.value = None
        
    @discord.ui.button(label="Huỷ giải cũ", style=discord.ButtonStyle.danger, custom_id="valorant:cancel_old")
    async def cancel_old(self, i: discord.Interaction, button: discord.ui.Button):
        guild_id = str(i.guild_id)
        if guild_id in valorant_data:
            del valorant_data[guild_id]
        self.value = True
        self.stop()
        await i.response.defer(ephemeral=False)
        try:
            await create_new_tournament(i)
        except Exception as e:
            print(f"Error creating new tournament: {e}")
            try:
                # Use followup if original response is still valid
                await i.followup.send("Đã xảy ra lỗi khi tạo giải đấu mới. Vui lòng thử lại.", ephemeral=True)
            except:
                # If original interaction expired, try to send a new message
                channel = i.channel
                if channel:
                    await channel.send(f"{i.user.mention}, đã xảy ra lỗi khi tạo giải đấu mới. Vui lòng thử lại.")
                
    @discord.ui.button(label="Giữ giải cũ", style=discord.ButtonStyle.secondary, custom_id="valorant:keep_old")
    async def keep_old(self, i: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await i.response.defer()

# ---------------- KẾT THÚC CHỨC NĂNG VALORANT ----------------
# End của phần Valorant custom

# ---------------- THỐNG KÊ HOẠT ĐỘNG ----------------
# Tin nhắn, voice và game
message_counts = {}      # {guild_id: {user_id: count}}
cumulative_voice = {}    # {guild_id: {user_id: total_seconds}}
weekly_voice = {}        # {guild_id: {user_id: total_seconds}}
cumulative_game = {}     # {guild_id: {user_id: {game_name: total_seconds}}}
weekly_game = {}         # {guild_id: {user_id: {game_name: total_seconds}}}
active_game = {}         # {guild_id: {user_id: {game_name: join_time}}}

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    guild_id = str(message.guild.id)
    user_id = str(message.author.id)
    if guild_id not in message_counts:
        message_counts[guild_id] = {}
    message_counts[guild_id][user_id] = message_counts[guild_id].get(user_id, 0) + 1

    channel_id = message.channel.id
    if guild_id in bot.speak_channel and bot.speak_channel[guild_id] == channel_id:
        voice_client = message.guild.voice_client
        if voice_client and voice_client.is_connected() and not voice_client.is_playing():
            try:
                file_path = f"speak_msg_{guild_id}_{int(time.time())}.mp3"
                tts = gTTS(text=message.content, lang='vi')
                tts.save(file_path)
                
                def after_playing(error):
                    if error:
                        print(f"TTS Error in message: {error}")
                    # Delete the file after playing
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
            except Exception as e:
                print(f"TTS Error: {e}")
                if 'file_path' in locals() and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
    # Thay thế đoạn xử lý trong on_message với phần này để xử lý đồng thời
        if guild_id in watchlist and str(message.channel.id) in watchlist[guild_id]:
            if message.attachments:
                # Xử lý tất cả các file cùng một lúc
                backup_tasks = []
                for attachment in message.attachments:
                    ext = os.path.splitext(attachment.filename)[1].lower()
                    if ext in image_extensions or ext in video_extensions:
                        # Tạo task cho mỗi file
                        task = asyncio.create_task(upload_attachment_to_dropbox(attachment, message.channel.name))
                        backup_tasks.append((attachment.filename, task))
                
                # Chờ tất cả các task hoàn thành
                if backup_tasks:
                    # Chờ tất cả hoàn thành
                    backed_up_files = []
                    for filename, task in backup_tasks:
                        try:
                            result = await task
                            if result:
                                backed_up_files.append(filename)
                        except Exception as e:
                            print(f"Lỗi backup {filename}: {e}")
                    
                    # Gửi thông báo gộp nếu có file được backup
                    if backed_up_files:
                        shore_chan = get_shore_channel(guild_id) or message.channel
                        if len(backed_up_files) == 1:
                            await shore_chan.send(f"Shore đã backup thành công file `{backed_up_files[0]}` từ channel {message.channel.mention} :>")
                        else:
                            files_list = ", ".join(f"`{f}`" for f in backed_up_files)
                            await shore_chan.send(f"Shore đã backup thành công {len(backed_up_files)} file: {files_list} từ channel {message.channel.mention} :>")
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member, before, after):
    guild_id = str(member.guild.id)
    user_id = str(member.id)
    now = datetime.now()
    
    # Voice tracking - Fix for AttributeError by using a dictionary instead of member attribute
    if before.channel is None and after.channel is not None:
        # User joined a voice channel
        if guild_id not in voice_join_times:
            voice_join_times[guild_id] = {}
        voice_join_times[guild_id][user_id] = now
    elif before.channel is not None and after.channel is None:
        # User left a voice channel
        if guild_id in voice_join_times and user_id in voice_join_times[guild_id]:
            join_time = voice_join_times[guild_id].pop(user_id)
            duration = (now - join_time).total_seconds()
            cumulative_voice.setdefault(guild_id, {})[user_id] = cumulative_voice.get(guild_id, {}).get(user_id, 0) + duration
            weekly_voice.setdefault(guild_id, {})[user_id] = weekly_voice.get(guild_id, {}).get(user_id, 0) + duration
    
    # Handle autojoin if enabled
    if after.channel is not None and guild_id in bot.autojoin_enabled and bot.autojoin_enabled[guild_id]:
        if member.guild.voice_client is None:  # Bot is not in any voice channel
            try:
                # Only join if we're not already in a voice channel
                voice_client = await after.channel.connect(timeout=10.0, reconnect=True)
                
                # Wait a short time to ensure connection is stable
                await asyncio.sleep(1.5)
                
                # Only play greeting if connection is successful and stable
                if voice_client.is_connected():
                    greeting = random.choice(greeting_phrases)
                    file_path = f"welcome_{guild_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=greeting, lang='vi')
                    tts.save(file_path)
                    
                    def after_playing(error):
                        if error:
                            print(f"Voice greeting error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
            except asyncio.TimeoutError:
                print(f"Timeout connecting to voice channel in guild {guild_id}")
            except Exception as e:
                print(f"Auto-join error: {e}")
    
    # Fix voice announcements for both join and leave
    if guild_id in bot.voice_announcements and bot.voice_announcements[guild_id]:
        vc = member.guild.voice_client
        # Only proceed if voice client exists, is connected, and not in the middle of connecting
        if vc and vc.is_connected():
            announce_text = None
            if before.channel is None and after.channel is not None:
                announce_text = f"{member.display_name} đã vào."
            elif before.channel is not None and after.channel is None:
                announce_text = f"{member.display_name} đã đi."
            
            if announce_text:
                try:
                    # Create a unique file name to prevent conflicts
                    file_path = f"voice_info_{user_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=announce_text, lang='vi')
                    tts.save(file_path)
                    
                    # Function to handle cleanup after playing
                    def after_playing(error):
                        if error:
                            print(f"Voice announcement error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    # Play audio with cleanup callback
                    if not vc.is_playing():
                        vc.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
                    else:
                        # Add to queue with a helper function
                        asyncio.create_task(play_when_free(vc, file_path))
                except Exception as e:
                    print(f"Voice announce error: {e}")
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass

# Helper function to play audio when voice client is free
async def play_when_free(voice_client, file_path, max_wait=15.0):
    """Wait for voice client to be free and then play the audio file"""
    try:
        start_time = time.time()
        
        # Wait for the current audio to finish (with timeout)
        while voice_client.is_playing() and time.time() - start_time < max_wait:
            await asyncio.sleep(0.5)
        
        # Check if we're still connected
        if voice_client.is_connected():
            def after_playing(error):
                if error:
                    print(f"Voice playback error: {error}")
                # Delete the file after playing
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
                
            voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
        else:
            # Clean up file if we can't play it
            if os.path.exists(file_path):
                os.remove(file_path)
    except Exception as e:
        print(f"Error in play_when_free: {e}")
        # Clean up file if there's an error
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

# ---------------- THỐNG KÊ HÀNG NGÀY VÀ TUẦN ----------------
# Các lệnh thống kê /stats, /setstatstime, daily_stats_task, weekly_stats_task như cũ
@bot.tree.command(name="stats", description="Hiển thị thống kê hoạt động của server")
async def stats(interaction: discord.Interaction, start_date: str = None, end_date: str = None):
    guild_id = str(interaction.guild.id)
    msg_stats = message_counts.get(guild_id, {})
    total_messages = sum(msg_stats.values())
    active_users = len(msg_stats)
    top_chat = sorted(msg_stats.items(), key=lambda x: x[1], reverse=True)[:5]
    top_chat_str = "\n".join(f"<@{uid}>: {cnt} tin" for uid, cnt in top_chat) if top_chat else "Chưa có dữ liệu."
    def format_duration(seconds):
        s = int(seconds)
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        return f"{h}h {m}m {s}s"
    voice_stats = cumulative_voice.get(guild_id, {}).copy()
    top_voice = sorted(voice_stats.items(), key=lambda x: x[1], reverse=True)[:5]
    top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
    game_stats = {}
    for uid, games in cumulative_game.get(guild_id, {}).items():
        game_stats[uid] = sum(games.values())
    top_game = sorted(game_stats.items(), key=lambda x: x[1], reverse=True)[:5]
    top_game_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_game) if top_game else "Chưa có dữ liệu."
    embed = discord.Embed(
        title="Thống kê của Shore",
        description=f"Tổng tin nhắn: {total_messages}\nSố thành viên tương tác: {active_users}",
        color=0x3498db,
        timestamp=datetime.now()
    )
    embed.add_field(name="Top 5 người chat nhiều nhất", value=top_chat_str, inline=False)
    embed.add_field(name="Top 5 voice chat (thời gian lâu nhất)", value=top_voice_str, inline=False)
    embed.add_field(name="Top 5 game (thời gian chơi nhiều nhất)", value=top_game_str, inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=False)

# Lệnh /setstatstime để đặt giờ gửi thống kê hàng ngày (mặc định 23:00)
daily_stats_time = "23:00"

@bot.tree.command(name="setstatstime", description="Đặt giờ tự động gửi thống kê hàng ngày (HH:MM)")
async def setstatstime(interaction: discord.Interaction, time_str: str):
    global daily_stats_time
    try:
        datetime.strptime(time_str, "%H:%M")
        daily_stats_time = time_str
        await interaction.response.send_message(f"Shore đã đặt giờ thống kê hàng ngày là {daily_stats_time}", ephemeral=True)
    except ValueError:
        await interaction.response.send_message("Định dạng giờ không hợp lệ. Hãy dùng HH:MM (ví dụ: 23:00).", ephemeral=True)

async def daily_stats_task():
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now()
        target = datetime.strptime(daily_stats_time, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        if target < now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        for guild in bot.guilds:
            guild_id = str(guild.id)
            shore_chan = get_shore_channel(guild_id)
            if shore_chan:
                msg_stats = message_counts.get(guild_id, {})
                total_messages = sum(msg_stats.values())
                active_users = len(msg_stats)
                top_chat = sorted(msg_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_chat_str = "\n".join(f"<@{uid}>: {cnt} tin" for uid, cnt in top_chat) if top_chat else "Chưa có dữ liệu."
                def format_duration(seconds):
                    s = int(seconds)
                    h, s = divmod(s, 3600)
                    m, s = divmod(s, 60)
                    return f"{h}h {m}m {s}s"
                voice_stats = cumulative_voice.get(guild_id, {}).copy()
                top_voice = sorted(voice_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                game_stats = {}
                for uid, games in cumulative_game.get(guild_id, {}).items():
                    game_stats[uid] = sum(games.values())
                top_game = sorted(game_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_game_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_game) if top_game else "Chưa có dữ liệu."
                embed = discord.Embed(
                    title="Thống kê hàng ngày của Shore",
                    description=f"Tổng tin nhắn: {total_messages}\nSố thành viên tương tác: {active_users}",
                    color=0x3498db,
                    timestamp=datetime.now()
                )
                embed.add_field(name="Top 5 người chat nhiều nhất", value=top_chat_str, inline=False)
                embed.add_field(name="Top 5 voice chat (thời gian lâu nhất)", value=top_voice_str, inline=False)
                embed.add_field(name="Top 5 game (thời gian chơi nhiều nhất)", value=top_game_str, inline=False)
                await shore_chan.send(embed=embed)

async def weekly_stats_task():
    global weekly_voice, weekly_game
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now()
        days_ahead = (7 - now.weekday()) % 7
        if now.weekday() == 0 and now.time() > datetime.strptime("00:00", "%H:%M").time():
            days_ahead = 7
        target = datetime.combine(now.date() + timedelta(days=days_ahead), datetime.min.time())
        await asyncio.sleep((target - now).total_seconds())
        for guild in bot.guilds:
            guild_id = str(guild.id)
            shore_chan = get_shore_channel(guild_id)
            if shore_chan:
                def format_duration(seconds):
                    s = int(seconds)
                    h, s = divmod(s, 3600)
                    m, s = divmod(s, 60)
                    return f"{h}h {m}m {s}s"
                v_stats = weekly_voice.get(guild_id, {}).copy()
                top_voice = sorted(v_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                g_stats = {}
                for uid, games in weekly_game.get(guild_id, {}).items():
                    g_stats[uid] = sum(games.values())
                top_game = sorted(g_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_game_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_game) if top_game else "Chưa có dữ liệu."
                embed = discord.Embed(
                    title="Bảng xếp hạng Game (Tuần)",
                    description="Top 5 người có thời gian chơi game nhiều nhất tuần qua",
                    color=0x3498db,
                    timestamp=datetime.now()
                )
                embed.add_field(name="Top 5 game", value=top_game_str, inline=False)
                v_stats = weekly_voice.get(guild_id, {}).copy()
                top_voice = sorted(v_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                embed.add_field(name="Top 5 voice chat (tuần)", value=top_voice_str, inline=False)
                await shore_chan.send(embed=embed)
        weekly_voice = {}
        weekly_game = {}

# ---------------- Global toggle dictionaries ----------------
bot.speak_channel = {}       
bot.voice_announcements = {}
bot.autojoin_enabled = {}  # Track whether autojoin is enabled for each guild

# Dictionary to store voice join times (avoids setting attributes on Member objects)
voice_join_times = {}  # {guild_id: {user_id: datetime}}

# ---------------- Valorant Custom Functionality ----------------
# Tạo dữ liệu giải Valorant cho mỗi guild
valorant_data = {}  # {guild_id: {match_date, team1, team2, guest_mode, map, side, mode, score}}

# ---------------- THỐNG KÊ HÀNG NGÀY VÀ TUẦN ----------------
# Các thống kê tin nhắn, voice, game
message_counts = {}      # {guild_id: {user_id: count}}
cumulative_voice = {}    # {guild_id: {user_id: total_seconds}}
weekly_voice = {}        # {guild_id: {user_id: total_seconds}}
cumulative_game = {}     # {guild_id: {user_id: {game_name: total_seconds}}}
weekly_game = {}         # {guild_id: {user_id: {game_name: total_seconds}}}
active_game = {}         # {guild_id: {user_id: {game_name: join_time}}}

@bot.event
async def on_presence_update(before, after):
    guild = after.guild
    guild_id = str(guild.id)
    user_id = str(after.id)
    now = datetime.now()
    before_games = set(activity.name for activity in before.activities if isinstance(activity, discord.Game))
    after_games = set(activity.name for activity in after.activities if isinstance(activity, discord.Game))
    started_games = after_games - before_games
    ended_games = before_games - after_games
    active_game.setdefault(guild_id, {})
    cumulative_game.setdefault(guild_id, {})
    weekly_game.setdefault(guild_id, {})
    for game in started_games:
        active_game[guild_id].setdefault(user_id, {})[game] = now
    for game in ended_games:
        if user_id in active_game[guild_id] and game in active_game[guild_id][user_id]:
            start_time = active_game[guild_id][user_id].pop(game)
            duration = (now - start_time).total_seconds()
            cumulative_game[guild_id].setdefault(user_id, {})[game] = cumulative_game[guild_id].get(user_id, {}).get(game, 0) + duration
            weekly_game[guild_id].setdefault(user_id, {})[game] = weekly_game[guild_id].get(user_id, {}).get(game, 0) + duration

@bot.event
async def on_voice_state_update(member, before, after):
    guild_id = str(member.guild.id)
    user_id = str(member.id)
    now = datetime.now()
    
    # Voice tracking - Fix for AttributeError by using a dictionary instead of member attribute
    if before.channel is None and after.channel is not None:
        # User joined a voice channel
        if guild_id not in voice_join_times:
            voice_join_times[guild_id] = {}
        voice_join_times[guild_id][user_id] = now
    elif before.channel is not None and after.channel is None:
        # User left a voice channel
        if guild_id in voice_join_times and user_id in voice_join_times[guild_id]:
            join_time = voice_join_times[guild_id].pop(user_id)
            duration = (now - join_time).total_seconds()
            cumulative_voice.setdefault(guild_id, {})[user_id] = cumulative_voice.get(guild_id, {}).get(user_id, 0) + duration
            weekly_voice.setdefault(guild_id, {})[user_id] = weekly_voice.get(guild_id, {}).get(user_id, 0) + duration
    
    # Handle autojoin if enabled
    if after.channel is not None and guild_id in bot.autojoin_enabled and bot.autojoin_enabled[guild_id]:
        if member.guild.voice_client is None:  # Bot is not in any voice channel
            try:
                # Only join if we're not already in a voice channel
                voice_client = await after.channel.connect(timeout=10.0, reconnect=True)
                
                # Wait a short time to ensure connection is stable
                await asyncio.sleep(1.5)
                
                # Only play greeting if connection is successful and stable
                if voice_client.is_connected():
                    greeting = random.choice(greeting_phrases)
                    file_path = f"welcome_{guild_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=greeting, lang='vi')
                    tts.save(file_path)
                    
                    def after_playing(error):
                        if error:
                            print(f"Voice greeting error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
            except asyncio.TimeoutError:
                print(f"Timeout connecting to voice channel in guild {guild_id}")
            except Exception as e:
                print(f"Auto-join error: {e}")
    
    # Fix voice announcements for both join and leave
    if guild_id in bot.voice_announcements and bot.voice_announcements[guild_id]:
        vc = member.guild.voice_client
        # Only proceed if voice client exists, is connected, and not in the middle of connecting
        if vc and vc.is_connected():
            announce_text = None
            if before.channel is None and after.channel is not None:
                announce_text = f"{member.display_name} đã đến."
            elif before.channel is not None and after.channel is None:
                announce_text = f"{member.display_name} đã đi."
            
            if announce_text:
                try:
                    # Create a unique file name to prevent conflicts
                    file_path = f"voice_info_{user_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=announce_text, lang='vi')
                    tts.save(file_path)
                    
                    # Function to handle cleanup after playing
                    def after_playing(error):
                        if error:
                            print(f"Voice announcement error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    # Play audio with cleanup callback
                    if not vc.is_playing():
                        vc.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
                    else:
                        # Add to queue with a helper function
                        asyncio.create_task(play_when_free(vc, file_path))
                except Exception as e:
                    print(f"Voice announce error: {e}")
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass

# Helper function to play audio when voice client is free
async def play_when_free(voice_client, file_path, max_wait=15.0):
    """Wait for voice client to be free and then play the audio file"""
    try:
        start_time = time.time()
        
        # Wait for the current audio to finish (with timeout)
        while voice_client.is_playing() and time.time() - start_time < max_wait:
            await asyncio.sleep(0.5)
        
        # Check if we're still connected
        if voice_client.is_connected():
            def after_playing(error):
                if error:
                    print(f"Voice playback error: {error}")
                # Delete the file after playing
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
                
            voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
        else:
            # Clean up file if we can't play it
            if os.path.exists(file_path):
                os.remove(file_path)
    except Exception as e:
        print(f"Error in play_when_free: {e}")
        # Clean up file if there's an error
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

daily_stats_time = "23:00"

async def daily_stats_task():
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now()
        target = datetime.strptime(daily_stats_time, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        if target < now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        for guild in bot.guilds:
            guild_id = str(guild.id)
            shore_chan = get_shore_channel(guild_id)
            if shore_chan:
                msg_stats = message_counts.get(guild_id, {})
                total_messages = sum(msg_stats.values())
                active_users = len(msg_stats)
                top_chat = sorted(msg_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_chat_str = "\n".join(f"<@{uid}>: {cnt} tin" for uid, cnt in top_chat) if top_chat else "Chưa có dữ liệu."
                def format_duration(seconds):
                    s = int(seconds)
                    h, s = divmod(s, 3600)
                    m, s = divmod(s, 60)
                    return f"{h}h {m}m {s}s"
                voice_stats = cumulative_voice.get(guild_id, {}).copy()
                top_voice = sorted(voice_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                game_stats = {}
                for uid, games in cumulative_game.get(guild_id, {}).items():
                    game_stats[uid] = sum(games.values())
                top_game = sorted(game_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_game_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_game) if top_game else "Chưa có dữ liệu."
                embed = discord.Embed(
                    title="Thống kê hàng ngày của Shore",
                    description=f"Tổng tin nhắn: {total_messages}\nSố thành viên tương tác: {active_users}",
                    color=0x3498db,
                    timestamp=datetime.now()
                )
                embed.add_field(name="Top 5 người chat nhiều nhất", value=top_chat_str, inline=False)
                embed.add_field(name="Top 5 voice chat (thời gian lâu nhất)", value=top_voice_str, inline=False)
                embed.add_field(name="Top 5 game (thời gian chơi nhiều nhất)", value=top_game_str, inline=False)
                await shore_chan.send(embed=embed)

async def weekly_stats_task():
    global weekly_voice, weekly_game
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.now()
        days_ahead = (7 - now.weekday()) % 7
        if now.weekday() == 0 and now.time() > datetime.strptime("00:00", "%H:%M").time():
            days_ahead = 7
        target = datetime.combine(now.date() + timedelta(days=days_ahead), datetime.min.time())
        await asyncio.sleep((target - now).total_seconds())
        for guild in bot.guilds:
            guild_id = str(guild.id)
            shore_chan = get_shore_channel(guild_id)
            if shore_chan:
                def format_duration(seconds):
                    s = int(seconds)
                    h, s = divmod(s, 3600)
                    m, s = divmod(s, 60)
                    return f"{h}h {m}m {s}s"
                v_stats = weekly_voice.get(guild_id, {}).copy()
                top_voice = sorted(v_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                g_stats = {}
                for uid, games in weekly_game.get(guild_id, {}).items():
                    g_stats[uid] = sum(games.values())
                top_game = sorted(g_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_game_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_game) if top_game else "Chưa có dữ liệu."
                embed = discord.Embed(
                    title="Bảng xếp hạng Game (Tuần)",
                    description="Top 5 người có thời gian chơi game nhiều nhất tuần qua",
                    color=0x3498db,
                    timestamp=datetime.now()
                )
                embed.add_field(name="Top 5 game", value=top_game_str, inline=False)
                v_stats = weekly_voice.get(guild_id, {}).copy()
                top_voice = sorted(v_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                top_voice_str = "\n".join(f"<@{uid}>: {format_duration(duration)}" for uid, duration in top_voice) if top_voice else "Chưa có dữ liệu."
                embed.add_field(name="Top 5 voice chat (tuần)", value=top_voice_str, inline=False)
                await shore_chan.send(embed=embed)
        weekly_voice = {}
        weekly_game = {}

# ---------------- Global toggle dictionaries ----------------
bot.speak_channel = {}       
bot.voice_announcements = {}
bot.autojoin_enabled = {}  # Track whether autojoin is enabled for each guild

# Dictionary to store voice join times (avoids setting attributes on Member objects)
voice_join_times = {}  # {guild_id: {user_id: datetime}}

# ---------------- COMMANDS ----------------
@bot.tree.command(name="shorechannel", description="Chọn channel để Shore gửi tin nhắn và log backup")
async def shorechannel(interaction: discord.Interaction, channel: discord.TextChannel):
    guild_id = str(interaction.guild_id)
    shore_channels[guild_id] = str(channel.id)
    save_json(shore_channels, SHORE_CHANNEL_FILE)
    await interaction.response.send_message(f"Shore sẽ gửi tin nhắn và log backup vào channel {channel.mention}.", ephemeral=True)

@bot.tree.command(name="watch", description="Thêm channel hiện tại vào danh sách theo dõi để backup file")
async def watch(interaction: discord.Interaction):
    guild_id = str(interaction.guild_id)
    channel_id = str(interaction.channel_id)
    if guild_id not in watchlist:
        watchlist[guild_id] = []
    if channel_id not in watchlist[guild_id]:
        watchlist[guild_id].append(channel_id)
        save_json(watchlist, WATCHLIST_FILE)
        shore_chan = get_shore_channel(guild_id) or interaction.channel
        await shore_chan.send(f"Shore sẽ theo dõi channel <#{channel_id}>!")
        await interaction.response.send_message("Thông báo đã được gửi vào Shore channel.", ephemeral=True)
    else:
        await interaction.response.send_message("Channel này đã được Shore theo dõi.", ephemeral=True)

@bot.tree.command(name="unwatch", description="Xóa channel hiện tại khỏi danh sách theo dõi")
async def unwatch(interaction: discord.Interaction):
    guild_id = str(interaction.guild_id)
    channel_id = str(interaction.channel_id)
    if guild_id in watchlist and channel_id in watchlist[guild_id]:
        watchlist[guild_id].remove(channel_id)
        save_json(watchlist, WATCHLIST_FILE)
        shore_chan = get_shore_channel(guild_id) or interaction.channel
        await shore_chan.send(f"Shore đã ngừng theo dõi channel <#{channel_id}>.")
        await interaction.response.send_message("Thông báo đã được gửi vào Shore channel.", ephemeral=True)
    else:
        await interaction.response.send_message("Channel này chưa được Shore theo dõi.", ephemeral=True)

@bot.tree.command(name="watchstatus", description="Kiểm tra các channel đang được Shore theo dõi")
async def watchstatus(interaction: discord.Interaction):
    guild_id = str(interaction.guild_id)
    shore_chan = get_shore_channel(guild_id) or interaction.channel
    if guild_id in watchlist and watchlist[guild_id]:
        channels = ", ".join(f"<#{cid}>" for cid in watchlist[guild_id])
        await shore_chan.send(f"Shore đang theo dõi các channel: {channels}")
    else:
        await shore_chan.send("Shore chưa được cài đặt theo dõi channel nào trong server này.")
    await interaction.response.send_message("Thông báo đã được gửi vào Shore channel.", ephemeral=True)

@bot.tree.command(name="backupstatus", description="Kiểm tra trạng thái backup và thống kê")
async def backupstatus(interaction: discord.Interaction):
    """Hiển thị thống kê về các file đã backup"""
    guild_id = str(interaction.guild.id)
    
    await interaction.response.defer(ephemeral=False)
    
    try:
        # Kết nối Dropbox
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET
        )
        
        # Lấy danh sách tất cả các thư mục backup
        result = dbx.files_list_folder('')
        folders = [entry.name for entry in result.entries if isinstance(entry, dropbox.files.FolderMetadata)]
        
        # Tạo thống kê cho mỗi thư mục
        stats = {}
        total_files = 0
        total_size = 0
        
        for folder in folders:
            try:
                # Lấy danh sách thư mục con (images, videos)
                subfolders = []
                try:
                    subfolder_result = dbx.files_list_folder(f'/{folder}')
                    subfolders = [entry.name for entry in subfolder_result.entries 
                                if isinstance(entry, dropbox.files.FolderMetadata)]
                except:
                    pass
                
                folder_stats = {'files': 0, 'size': 0, 'subfolders': {}}
                
                # Thống kê file trực tiếp trong thư mục
                try:
                    folder_content = dbx.files_list_folder(f'/{folder}')
                    for entry in folder_content.entries:
                        if isinstance(entry, dropbox.files.FileMetadata):
                            folder_stats['files'] += 1
                            folder_stats['size'] += entry.size
                            total_files += 1
                            total_size += entry.size
                except:
                    pass
                
                # Thống kê trong các thư mục con
                for subfolder in subfolders:
                    subfolder_stats = {'files': 0, 'size': 0}
                    
                    try:
                        subfolder_content = dbx.files_list_folder(f'/{folder}/{subfolder}')
                        for entry in subfolder_content.entries:
                            if isinstance(entry, dropbox.files.FileMetadata):
                                subfolder_stats['files'] += 1
                                subfolder_stats['size'] += entry.size
                                folder_stats['files'] += 1
                                folder_stats['size'] += entry.size
                                total_files += 1
                                total_size += entry.size
                    except:
                        pass
                    
                    folder_stats['subfolders'][subfolder] = subfolder_stats
                
                stats[folder] = folder_stats
                
            except Exception as e:
                print(f"Error getting stats for folder {folder}: {e}")
        
        # Tạo embed
        embed = discord.Embed(
            title="📊 Thống kê backup",
            description=f"Tổng cộng: **{total_files}** file - {format_size(total_size)}",
            color=0x3498db,
            timestamp=datetime.now()
        )
        
        # Thêm thông tin chi tiết cho mỗi channel
        for folder, folder_stats in sorted(stats.items(), key=lambda x: x[1]['files'], reverse=True)[:10]:
            images_count = folder_stats['subfolders'].get('images', {}).get('files', 0)
            videos_count = folder_stats['subfolders'].get('videos', {}).get('files', 0)
            others_count = folder_stats['files'] - images_count - videos_count
            
            detail = f"**{folder_stats['files']}** file - {format_size(folder_stats['size'])}\n"
            if images_count > 0:
                detail += f"🖼️ {images_count} ảnh\n"
            if videos_count > 0:
                detail += f"🎬 {videos_count} video\n"
            if others_count > 0:
                detail += f"📄 {others_count} file khác"
            
            embed.add_field(name=f"#{folder}", value=detail, inline=True)
        
        # Nếu có nhiều hơn 10 thư mục
        if len(stats) > 10:
            embed.set_footer(text=f"Hiển thị 10/{len(stats)} channel có nhiều file nhất")
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi lấy thống kê: {str(e)}")

def get_dropbox_client():
    """Lấy hoặc tạo client Dropbox một lần duy nhất"""
    global _dropbox_client
    if _dropbox_client is None:
        _dropbox_client = dropbox.Dropbox(
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET
        )
    return _dropbox_client

def format_size(size_bytes):
    """Format file size in bytes to human-readable format"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.1f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.1f} GB"

@bot.tree.command(name="backupall", description="Backup toàn bộ file ảnh/video của channel")
async def backupall(interaction: discord.Interaction, from_timestamp: str = None):
    channel = interaction.channel
    view = ConfirmBackupView()
    await interaction.response.send_message("Cậu có chắc chắn muốn Shore backup toàn bộ file không? Nếu backup từ thời điểm cụ thể, nhập theo định dạng `YYYY-MM-DD HH:MM`.", view=view, ephemeral=True)
    await view.wait()
    if not view.value:
        await interaction.followup.send("Shore đã huỷ Backup.", ephemeral=True)
        return
    dt = None
    if from_timestamp:
        try:
            dt = datetime.strptime(from_timestamp, "%Y-%m-%d %H:%M")
        except ValueError:
            await interaction.followup.send("Shore thấy cậu ghi sai định dạng thời gian. Hãy ghi theo kiểu 'YYYY-MM-DD HH:MM' nya~", ephemeral=True)
            return
    count = 0
    async for msg in channel.history(limit=None, after=dt):
        if msg.attachments:
            for attachment in msg.attachments:
                ext = os.path.splitext(attachment.filename)[1].lower()
                if ext in image_extensions or ext in video_extensions:
                    result = await upload_attachment_to_dropbox(attachment, channel.name)
                    if result:
                        count += 1
    await interaction.followup.send(f"Shore đã backup xong {count} file.", ephemeral=True)
    guild_id = str(interaction.guild_id)
    shore_chan = get_shore_channel(guild_id) or interaction.channel
    await shore_chan.send(f"Shore đã backup {count} file từ channel {channel.mention}:<")

@bot.tree.command(name="plan", description="Tạo kế hoạch đi chơi và vote tham gia (tag @everyone)")
async def plan(interaction: discord.Interaction, event_name: str, day: int, hour: int, minute: int, month: int = None, year: int = None, description: str = None):
    now = datetime.now()
    if not year:
        year = now.year
    if not month:
        month = now.month
    try:
        event_dt = datetime(year, month, day, hour, minute)
    except ValueError:
        await interaction.response.send_message("Thời gian không hợp lệ. Hãy kiểm tra lại các giá trị ngày, giờ, phút.", ephemeral=True)
        return
    if event_dt <= now:
        await interaction.response.send_message("Thời gian phải là tương lai, cậu ơi.", ephemeral=True)
        return
    # Trong phần tạo embed của lệnh plan
    embed = discord.Embed(title=event_name, description=description or "", color=0x3498db)
    embed.add_field(name="Giờ bắt đầu", value=event_dt.strftime("%Y-%m-%d %H:%M"), inline=False)

    # Thêm các thông tin hữu ích khác
    time_until = event_dt - datetime.now()
    days, seconds = time_until.days, time_until.seconds
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    time_text = ""
    if days > 0:
        time_text += f"{days} ngày "
    if hours > 0:
        time_text += f"{hours} giờ "
    time_text += f"{minutes} phút nữa"

    embed.add_field(name="Thời gian còn lại", value=time_text, inline=False)
    embed.add_field(name="Người tham gia", value="@everyone\nChưa có ai tham gia", inline=False)

    # Thêm hình ảnh dựa trên loại sự kiện (nếu có)
    if "đi chơi" in event_name.lower() or "café" in event_name.lower():
        embed.set_thumbnail(url="https://i.imgur.com/uZIlRnK.png")
    elif "game" in event_name.lower():
        embed.set_thumbnail(url="https://i.imgur.com/JhzC3vM.png")
    elif "học" in event_name.lower():
        embed.set_thumbnail(url="https://i.imgur.com/9NXfzpF.png")
        
    plan_id = f"plan_{interaction.id}"
    view = PlanView(plan_id=plan_id)
    bot.add_view(view)
    shore_chan = get_shore_channel(str(interaction.guild_id)) or interaction.channel
    message = await shore_chan.send(embed=embed, view=view, ephemeral=False)
    plans[plan_id] = {
        "event_name": event_name,
        "event_time": event_dt.isoformat(),
        "description": description,
        "channel_id": shore_chan.id,
        "message_id": message.id,
        "voters": [],
        "creator_id": interaction.user.id,  # Lưu người tạo
        "created_at": datetime.now().isoformat(),  # Thời gian tạo sự kiện
    }
    save_json(plans, PLANS_FILE)
    asyncio.create_task(auto_delete_expired_plan(plan_id, event_dt + timedelta(hours=2)))  # Xóa sau 2 giờ khi sự kiện kết thúc
    asyncio.create_task(schedule_reminder(shore_chan.id, plan_id, event_dt))
    await interaction.response.send_message("Kế hoạch đã được gửi vào Shore channel.", ephemeral=True)
    

@bot.tree.command(name="inviteguest", description="Tạo link mời khách vào voice hiện tại")
async def inviteguest(interaction: discord.Interaction):
    if interaction.user.voice is None or interaction.user.voice.channel is None:
        await interaction.response.send_message("Cậu đang không ở trong voice chat nào để Shore vào cả", ephemeral=True)
        return
    voice_channel = interaction.user.voice.channel
    if interaction.guild.voice_client is None:
        await voice_channel.connect()
    invite = await voice_channel.create_invite(max_age=300, max_uses=1, unique=True)
    guild_id = str(interaction.guild_id)
    shore_chan = get_shore_channel(guild_id) or interaction.channel
    await shore_chan.send(f"Link mời guest vào voice: {invite.url}")
    await interaction.response.send_message("Link mời đã được gửi vào Shore channel.", ephemeral=True)

@bot.tree.command(name="connect", description="Shore tham gia voice channel của cậu")
async def connect(interaction: discord.Interaction):
    if interaction.user.voice is None or interaction.user.voice.channel is None:
        await interaction.response.send_message("Cậu đang không ở trong voice chat nào để Shore vào cả", ephemeral=True)
        return
    voice_channel = interaction.user.voice.channel
    if interaction.guild.voice_client is not None:
        await interaction.response.send_message(f"Shore đã ở trong voice channel {interaction.guild.voice_client.channel.mention}", ephemeral=True)
        return
    try:
        await voice_channel.connect()
        greeting = random.choice(greeting_phrases)
        tts = gTTS(text=greeting, lang='vi')
        tts.save("welcome.mp3")
        interaction.guild.voice_client.play(discord.FFmpegPCMAudio("welcome.mp3"))
        await interaction.response.send_message(f"Shore đã tham gia vào voice channel {voice_channel.mention}", ephemeral=True)
    except Exception as e:
        print(e)
        await interaction.response.send_message("Đã xảy ra lỗi: " + str(e), ephemeral=True)

@bot.tree.command(name="disconnect", description="Shore rời khỏi voice channel hiện tại")
async def disconnect(interaction: discord.Interaction):
    if interaction.guild.voice_client is None:
        await interaction.response.send_message("Shore không ở trong voice channel nào cả", ephemeral=True)
        return
    try:
        await interaction.guild.voice_client.disconnect()
        await interaction.response.send_message("Shore đã rời khỏi voice channel.", ephemeral=True)
    except Exception:
        await interaction.response.send_message("Đã xảy ra lỗi khi Shore cố gắng rời khỏi voice.", ephemeral=True)

@bot.tree.command(name="speak", description="Bật/tắt chế độ đọc tin nhắn trong channel hoặc nói câu cụ thể")
async def speak(interaction: discord.Interaction, message: str = None):
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    if message:
        if not voice_client or not voice_client.is_connected():
            await interaction.response.send_message("Shore cần được kết nối vào voice channel để nói.", ephemeral=True)
            return
        try:
            file_path = f"speak_{guild_id}_{int(time.time())}.mp3"
            tts = gTTS(text=message, lang='vi')
            tts.save(file_path)
            
            def after_playing(error):
                if error:
                    print(f"TTS Error: {error}")
                # Delete the file after playing
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
            
            voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
            await interaction.response.send_message("Shore đang nói...", ephemeral=True)
        except Exception as e:
            print(f"Speak command error: {e}")
            await interaction.response.send_message(f"Đã xảy ra lỗi: {str(e)}", ephemeral=True)
            if 'file_path' in locals() and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
    else:
        if interaction.user.voice is None or interaction.user.voice.channel is None:
            await interaction.response.send_message("Cậu đang không ở trong voice chat nào để Shore vào", ephemeral=True)
            return
        if guild_id in bot.speak_channel and bot.speak_channel[guild_id] == interaction.channel.id:
            del bot.speak_channel[guild_id]
            await interaction.response.send_message("Chế độ nói đã tắt.", ephemeral=True)
        else:
            if interaction.guild.voice_client is None:
                try:
                    await interaction.user.voice.channel.connect(timeout=10.0, reconnect=True)
                    await asyncio.sleep(1.0)  # Wait a moment to ensure connection is stable
                except Exception as e:
                    await interaction.response.send_message(f"Không thể kết nối voice: {str(e)}", ephemeral=True)
                    return
                
            if interaction.guild.voice_client and interaction.guild.voice_client.is_connected():
                bot.speak_channel[guild_id] = interaction.channel.id
                await interaction.response.send_message("Chế độ nói đã bật. Mọi tin nhắn trong channel này sẽ được Shore đọc lên.", ephemeral=True)
            else:
                await interaction.response.send_message("Shore không thể kết nối với voice channel.", ephemeral=True)

@bot.tree.command(name="voiceinout", description="Bật/tắt thông báo khi ai vào/ra voice channel")
async def voiceinout(interaction: discord.Interaction):
    guild_id = str(interaction.guild_id)
    if guild_id not in bot.voice_announcements:
        bot.voice_announcements[guild_id] = False
    bot.voice_announcements[guild_id] = not bot.voice_announcements[guild_id]
    status = "bật" if bot.voice_announcements[guild_id] else "tắt"
    await interaction.response.send_message(f"Shore đã {status} thông báo voice in/out.", ephemeral=True)

@bot.tree.command(name="autojoin", description="Bật/tắt chế độ tự động tham gia voice chat khi có người vào")
async def autojoin(interaction: discord.Interaction):
    """Toggle auto-joining voice channels when members are present"""
    guild_id = str(interaction.guild_id)
    
    # Initialize if not present
    if guild_id not in bot.autojoin_enabled:
        bot.autojoin_enabled[guild_id] = False
    
    # Toggle the setting
    bot.autojoin_enabled[guild_id] = not bot.autojoin_enabled[guild_id]
    
    # Respond with current status
    status = "bật" if bot.autojoin_enabled[guild_id] else "tắt"
    await interaction.response.send_message(
        f"Shore đã {status} chế độ tự động tham gia voice chat khi có người vào.", 
        ephemeral=True
    )

@bot.tree.command(name="valorantcustom", description="Tạo giải Valorant Custom")
async def valorantcustom(interaction: discord.Interaction, match_date: str = None, match_time: str = None):
    guild_id = str(interaction.guild_id)
    
    # Đầu tiên, trì hoãn phản hồi để tránh timeout
    await interaction.response.defer(ephemeral=False)
    
    # Check if there's an existing tournament and handle it
    if guild_id in valorant_data:
        # Ask if they want to cancel the existing tournament
        embed = discord.Embed(
            title="Valorant Custom Tournament - Xác nhận",
            description="Đã có một giải đấu Valorant được thiết lập cho server này. Bạn có muốn huỷ giải cũ và tạo giải mới?",
            color=0xFFA500  # Warning color
        )
        
        view = ConfirmNewTournamentView()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        
        # Wait for the view instead of returning immediately
        await view.wait()
        
        # If the user selected "Cancel old", it will be handled in the button callback
        # If the user selected "Keep old" or didn't respond, don't do anything else
        return
    
    # If no existing tournament, create a new one
    try:
        if match_date is None:
            match_date = datetime.now().strftime("%Y-%m-%d")
        if match_time is None:
            match_time = "20:00"  # Default time
            
        # Pass creator_id to the view
        view = ValorantRegistrationView(
            str(interaction.guild_id), 
            match_date, 
            match_time, 
            interaction.user.id  # Pass the creator's ID
        )
        # Create initial embed
        embed = discord.Embed(
            title="Valorant Custom Tournament",
            description=f"Thời gian diễn ra: {match_date} {match_time}",
            color=0xFD4553  # Valorant red color
        )
        embed.add_field(name="Đội 1", value="*Chưa có thành viên*", inline=True)
        embed.add_field(name="Đội 2", value="*Chưa có thành viên*", inline=True)
        embed.set_footer(text="Bấm các nút phía dưới để đăng ký vào đội")
        
        # Add Valorant logo thumbnail
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/f/fc/Valorant_logo_-_pink_color_version.svg/1200px-Valorant_logo_-_pink_color_version.svg.png")
        
        # Gửi embed và view chính
        message = await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        view.message = message  # Lưu message để cập nhật sau này
        
        # Register the view with the bot
        bot.add_view(view)
    except Exception as e:
        print(f"Error creating tournament: {e}")
        await interaction.followup.send("Đã xảy ra lỗi khi tạo giải đấu. Vui lòng thử lại sau.", ephemeral=True)

async def create_new_tournament(interaction: discord.Interaction, match_date: str = None, match_time: str = None):
    try:
        if match_date is None:
            match_date = datetime.now().strftime("%Y-%m-%d")
        if match_time is None:
            match_time = "20:00"  # Default time
            
        # Pass creator_id to the view
        view = ValorantRegistrationView(
            str(interaction.guild_id), 
            match_date, 
            match_time, 
            interaction.user.id  # Pass the creator's ID
        )
        # Create initial embed
        embed = discord.Embed(
            title="Valorant Custom Tournament",
            description=f"Thời gian diễn ra: {match_date} {match_time}",
            color=0xFD4553  # Valorant red color
        )
        embed.add_field(name="Đội 1", value="*Chưa có thành viên*", inline=True)
        embed.add_field(name="Đội 2", value="*Chưa có thành viên*", inline=True)
        embed.set_footer(text="Bấm các nút phía dưới để đăng ký vào đội")
        
        # Add Valorant logo thumbnail
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/f/fc/Valorant_logo_-_pink_color_version.svg/1200px-Valorant_logo_-_pink_color_version.svg.png")
        
        # Sau khi tạo view và send message
        match_setup_view = MatchSetupView(str(interaction.guild_id))
        # Chỉ bật nút chọn chế độ, vô hiệu hóa các nút khác
        for child in match_setup_view.children:
            if isinstance(child, discord.ui.Select) and child.custom_id == "valorant:select_mode":
                child.disabled = False
            else:
                child.disabled = True
        
        await interaction.followup.send("Vui lòng chọn chế độ đấu (BO1, BO3 hoặc BO5):", view=match_setup_view, ephemeral=False)

        # Register the view with the bot after we have access to the message
        bot.add_view(view)
        
    except Exception as e:
        print(f"Error creating new tournament: {e}")
        # Try to send an error message back through appropriate channels
        try:
            if interaction.response.is_done():
                if hasattr(interaction, 'followup'):
                    await interaction.followup.send("Đã xảy ra lỗi khi tạo giải đấu mới. Vui lòng thử lại.", ephemeral=True)
                else:
                    await interaction.channel.send(f"{interaction.user.mention}, đã xảy ra lỗi khi tạo giải đấu mới. Vui lòng thử lại.")
            else:
                await interaction.response.send_message("Đã xảy ra lỗi khi tạo giải đấu mới. Vui lòng thử lại.", ephemeral=True)
        except:
            # Last resort fallback
            pass
        raise

class PlanView(discord.ui.View):
    def __init__(self, plan_id: str):
        super().__init__(timeout=None)  # Persistent view
        self.plan_id = plan_id
        self.voters = set()
        
    @discord.ui.button(label="Tham gia", style=discord.ButtonStyle.primary, custom_id=f"plan:join")
    async def join_plan(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        user_id = str(interaction.user.id)
        if user_id not in self.voters:
            self.voters.add(user_id)
            if self.plan_id in plans:
                if "voters" not in plans[self.plan_id]:
                    plans[self.plan_id]["voters"] = []
                plans[self.plan_id]["voters"].append(user_id)
                save_json(plans, PLANS_FILE)
                
            # Update the message embed
            embed = interaction.message.embeds[0]
            if len(self.voters) == 1:
                participants = f"<@{user_id}>"
            else:
                participants = ", ".join(f"<@{voter}>" for voter in self.voters)
            
            # Find the field with "Người tham gia" and update it
            for i, field in enumerate(embed.fields):
                if field.name == "Người tham gia":
                    embed.set_field_at(i, name="Người tham gia", value=participants, inline=False)
                    break
            
            await interaction.message.edit(embed=embed)
            await interaction.response.send_message("Bạn đã tham gia kế hoạch này!", ephemeral=True)
        else:
            await interaction.response.send_message("Bạn đã tham gia kế hoạch này rồi!", ephemeral=True)
    @discord.ui.button(label="Huỷ sự kiện", style=discord.ButtonStyle.danger, custom_id=f"plan:cancel")
    async def cancel_plan(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Chỉ người tạo hoặc admin mới có thể hủy
        if interaction.user.id != self.creator_id and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Chỉ người tạo sự kiện hoặc quản trị viên mới có thể hủy.", ephemeral=True)
            return
            
        # Vô hiệu hóa tất cả nút
        for child in self.children:
            child.disabled = True
            
        # Cập nhật embed
        embed = interaction.message.embeds[0]
        embed.title = f"🚫 {embed.title} (ĐÃ HỦY)"
        embed.color = discord.Color.dark_gray()
        embed.set_footer(text="Sự kiện này đã bị hủy")
        
        # Cập nhật tin nhắn
        await interaction.message.edit(embed=embed, view=self)
        await interaction.response.send_message("✅ Sự kiện đã được hủy.", ephemeral=True)
        
        # Xóa khỏi plans json
        if self.plan_id in plans:
            del plans[self.plan_id]
            save_json(plans, PLANS_FILE)

    @discord.ui.button(label="Huỷ tham gia", style=discord.ButtonStyle.danger, custom_id=f"plan:leave")
    async def leave_plan(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        user_id = str(interaction.user.id)
        if user_id in self.voters:
            self.voters.remove(user_id)
            if self.plan_id in plans and "voters" in plans[self.plan_id]:
                if user_id in plans[self.plan_id]["voters"]:
                    plans[self.plan_id]["voters"].remove(user_id)
                save_json(plans, PLANS_FILE)
                
            # Update the message embed
            embed = interaction.message.embeds[0]
            if not self.voters:
                participants = "Chưa có ai tham gia"
            else:
                participants = ", ".join(f"<@{voter}>" for voter in self.voters)
            
            # Find the field with "Người tham gia" and update it
            for i, field in enumerate(embed.fields):
                if field.name == "Người tham gia":
                    embed.set_field_at(i, name="Người tham gia", value=participants, inline=False)
                    break
            
            await interaction.message.edit(embed=embed)
            await interaction.response.send_message("Bạn đã huỷ tham gia kế hoạch này!", ephemeral=True)
        else:
            await interaction.response.send_message("Bạn chưa tham gia kế hoạch này!", ephemeral=True)

class ConfirmBackupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        self.value = None

    @discord.ui.button(label="Xác nhận", style=discord.ButtonStyle.success, custom_id="backup:confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Huỷ", style=discord.ButtonStyle.danger, custom_id="backup:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        self.stop()
        await interaction.response.defer()

async def schedule_reminder(channel_id, plan_id, event_dt):
    # Tạo nhiều nhắc nhở: 1 ngày, 1 giờ và 10 phút trước sự kiện
    reminders = [
        {"time": event_dt - timedelta(days=1), "message": "sẽ diễn ra vào ngày mai"},
        {"time": event_dt - timedelta(hours=1), "message": "sẽ diễn ra trong 1 giờ nữa"},
        {"time": event_dt - timedelta(minutes=10), "message": "sắp diễn ra trong 10 phút nữa! Chuẩn bị!"}
    ]
    
    for reminder in reminders:
        now = datetime.now()
        # Chỉ gửi nhắc nhở ở tương lai
        if now < reminder["time"]:
            # Đợi đến thời điểm nhắc nhở
            await asyncio.sleep((reminder["time"] - now).total_seconds())
            
            # Kiểm tra sự kiện còn tồn tại không
            if plan_id in plans:
                try:
                    channel = bot.get_channel(int(channel_id))
                    if channel:
                        plan_data = plans[plan_id]
                        voters = plan_data.get("voters", [])
                        
                        if voters:
                            mentions = " ".join(f"<@{uid}>" for uid in voters)
                            await channel.send(
                                f"📢 **NHẮC NHỞ SỰ KIỆN**\n{mentions} sự kiện **{plan_data['event_name']}** {reminder['message']}!"
                            )
                        else:
                            await channel.send(f"📢 Sự kiện **{plan_data['event_name']}** {reminder['message']}, nhưng không có ai tham gia.")
                except Exception as e:
                    print(f"Error sending reminder: {e}")

# Hàm mới để tự động dọn dẹp sự kiện đã hết hạn
async def auto_delete_expired_plan(plan_id, expiry_time):
    now = datetime.now()
    if now < expiry_time:
        await asyncio.sleep((expiry_time - now).total_seconds())
    
    # Xóa sự kiện đã hết hạn
    if plan_id in plans:
        try:
            # Cập nhật tin nhắn thành "Đã kết thúc" nếu còn tồn tại
            channel_id = plans[plan_id].get("channel_id")
            message_id = plans[plan_id].get("message_id")
            
            if channel_id and message_id:
                channel = bot.get_channel(int(channel_id))
                if channel:
                    try:
                        message = await channel.fetch_message(int(message_id))
                        if message:
                            embed = message.embeds[0]
                            embed.title = f"✅ {embed.title} (ĐÃ KẾT THÚC)"
                            embed.color = discord.Color.dark_gray()
                            embed.set_footer(text="Sự kiện này đã kết thúc")
                            
                            # Vô hiệu hóa nút
                            for view in message.components:
                                for component in view.children:
                                    component.disabled = True
                                    
                            await message.edit(embed=embed, components=message.components)
                    except:
                        pass  # Tin nhắn có thể đã bị xóa
            
            # Xóa khỏi danh sách plans
            del plans[plan_id]
            save_json(plans, PLANS_FILE)
        except Exception as e:
            print(f"Error cleaning up expired plan: {e}")

# Add autojoin check to run periodically
async def check_voice_channels():
    """Check voice channels and join if someone is there and autojoin is enabled"""
    await bot.wait_until_ready()
    while not bot.is_closed():
        for guild in bot.guilds:
            guild_id = str(guild.id)
            
            # Skip if autojoin is not enabled for this guild
            if guild_id not in bot.autojoin_enabled or not bot.autojoin_enabled[guild_id]:
                continue
            
            # Skip if the bot is already in a voice channel
            if guild.voice_client is not None and guild.voice_client.is_connected():
                continue
            
            # Check if any voice channels have members
            for voice_channel in guild.voice_channels:
                if len(voice_channel.members) > 0:  # Someone is in this channel
                    try:
                        # Use a timeout for voice connection
                        voice_client = await voice_channel.connect(timeout=10.0, reconnect=True)
                        
                        # Wait a short time to ensure connection is stable
                        await asyncio.sleep(1.5)
                        
                        # Only play greeting if connection is successful and stable
                        if voice_client.is_connected():
                            # Create a unique file name
                            file_path = f"welcome_check_{guild_id}_{int(time.time())}.mp3"
                            greeting = random.choice(greeting_phrases)
                            tts = gTTS(text=greeting, lang='vi')
                            tts.save(file_path)
                            
                            def after_playing(error):
                                if error:
                                    print(f"Voice greeting error: {error}")
                                # Delete the file after playing
                                if os.path.exists(file_path):
                                    try:
                                        os.remove(file_path)
                                    except:
                                        pass
                            
                            voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
                        break  # Stop checking after joining one channel
                    except asyncio.TimeoutError:
                        print(f"Timeout connecting to voice channel in guild {guild_id}")
                    except Exception as e:
                        print(f"Auto-join error: {e}")
                        
        # Check every 30 seconds
        await asyncio.sleep(30)

# Khai báo mẫu câu cho greeting
greeting_phrases = [
    "Chào bọn mày",
    "Im mồm cho Shore giáng lâm",
    "Mấy thằng nghiện",
    "Nì ga",
    "Bố mày vào rồi đây",
    "Hello mấy đứa",
    "Nigga",
    "Lô",
    "Alo",
    "what's up anh em"
]

# ---------------- EVENT HANDLERS ----------------


@bot.event
async def on_presence_update(before, after):
    guild = after.guild
    guild_id = str(guild.id)
    user_id = str(after.id)
    now = datetime.now()
    before_games = set(activity.name for activity in before.activities if isinstance(activity, discord.Game))
    after_games = set(activity.name for activity in after.activities if isinstance(activity, discord.Game))
    started_games = after_games - before_games
    ended_games = before_games - after_games
    active_game.setdefault(guild_id, {})
    cumulative_game.setdefault(guild_id, {})
    weekly_game.setdefault(guild_id, {})
    for game in started_games:
        active_game[guild_id].setdefault(user_id, {})[game] = now
    for game in ended_games:
        if user_id in active_game[guild_id] and game in active_game[guild_id][user_id]:
            start_time = active_game[guild_id][user_id].pop(game)
            duration = (now - start_time).total_seconds()
            cumulative_game[guild_id].setdefault(user_id, {})[game] = cumulative_game[guild_id].get(user_id, {}).get(game, 0) + duration
            weekly_game[guild_id].setdefault(user_id, {})[game] = weekly_game[guild_id].get(user_id, {}).get(game, 0) + duration

@bot.event
async def on_voice_state_update(member, before, after):
    guild_id = str(member.guild.id)
    user_id = str(member.id)
    now = datetime.now()
    
    # Voice tracking - Fix for AttributeError by using a dictionary instead of member attribute
    if before.channel is None and after.channel is not None:
        # User joined a voice channel
        if guild_id not in voice_join_times:
            voice_join_times[guild_id] = {}
        voice_join_times[guild_id][user_id] = now
    elif before.channel is not None and after.channel is None:
        # User left a voice channel
        if guild_id in voice_join_times and user_id in voice_join_times[guild_id]:
            join_time = voice_join_times[guild_id].pop(user_id)
            duration = (now - join_time).total_seconds()
            cumulative_voice.setdefault(guild_id, {})[user_id] = cumulative_voice.get(guild_id, {}).get(user_id, 0) + duration
            weekly_voice.setdefault(guild_id, {})[user_id] = weekly_voice.get(guild_id, {}).get(user_id, 0) + duration
    
    # Handle autojoin if enabled
    if after.channel is not None and guild_id in bot.autojoin_enabled and bot.autojoin_enabled[guild_id]:
        if member.guild.voice_client is None:  # Bot is not in any voice channel
            try:
                # Only join if we're not already in a voice channel
                voice_client = await after.channel.connect(timeout=10.0, reconnect=True)
                
                # Wait a short time to ensure connection is stable
                await asyncio.sleep(1.5)
                
                # Only play greeting if connection is successful and stable
                if voice_client.is_connected():
                    greeting = random.choice(greeting_phrases)
                    file_path = f"welcome_{guild_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=greeting, lang='vi')
                    tts.save(file_path)
                    
                    def after_playing(error):
                        if error:
                            print(f"Voice greeting error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
            except asyncio.TimeoutError:
                print(f"Timeout connecting to voice channel in guild {guild_id}")
            except Exception as e:
                print(f"Auto-join error: {e}")
    
    # Fix voice announcements for both join and leave
    if guild_id in bot.voice_announcements and bot.voice_announcements[guild_id]:
        vc = member.guild.voice_client
        # Only proceed if voice client exists, is connected, and not in the middle of connecting
        if vc and vc.is_connected():
            announce_text = None
            if before.channel is None and after.channel is not None:
                announce_text = f"{member.display_name} đã đến."
            elif before.channel is not None and after.channel is None:
                announce_text = f"{member.display_name} đã đi."
            
            if announce_text:
                try:
                    # Create a unique file name to prevent conflicts
                    file_path = f"voice_info_{user_id}_{int(now.timestamp())}.mp3"
                    tts = gTTS(text=announce_text, lang='vi')
                    tts.save(file_path)
                    
                    # Function to handle cleanup after playing
                    def after_playing(error):
                        if error:
                            print(f"Voice announcement error: {error}")
                        # Delete the file after playing
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                            except:
                                pass
                    
                    # Play audio with cleanup callback
                    if not vc.is_playing():
                        vc.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
                    else:
                        # Add to queue with a helper function
                        asyncio.create_task(play_when_free(vc, file_path))
                except Exception as e:
                    print(f"Voice announce error: {e}")
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass

# Helper function to play audio when voice client is free
async def play_when_free(voice_client, file_path, max_wait=15.0):
    """Wait for voice client to be free and then play the audio file"""
    try:
        start_time = time.time()
        
        # Wait for the current audio to finish (with timeout)
        while voice_client.is_playing() and time.time() - start_time < max_wait:
            await asyncio.sleep(0.5)
        
        # Check if we're still connected
        if voice_client.is_connected():
            def after_playing(error):
                if error:
                    print(f"Voice playback error: {error}")
                # Delete the file after playing
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
                
            voice_client.play(discord.FFmpegPCMAudio(file_path), after=after_playing)
        else:
            # Clean up file if we can't play it
            if os.path.exists(file_path):
                os.remove(file_path)
    except Exception as e:
        print(f"Error in play_when_free: {e}")
        # Clean up file if there's an error
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

# ---------------- MUSIC COMMANDS ----------------
import yt_dlp
import asyncio
import re
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from urllib.parse import urlparse, parse_qs
import time
from googleapiclient.discovery import build

stream_url_cache = {} 

# Cache kết quả Invidious
invidious_cache = {}

async def fetch_from_invidious(video_id):
    """Lấy thông tin và URL stream từ Invidious API với cache"""
    now = time.time()
    
    # Kiểm tra cache
    if video_id in invidious_cache and now < invidious_cache[video_id].get('expires_at', 0):
        print(f"Sử dụng kết quả Invidious từ cache cho {video_id}")
        return invidious_cache[video_id]
    
    # Sử dụng tất cả instance song song thay vì tuần tự
    async def try_instance(instance):
        try:
            api_url = f"{instance}/api/v1/videos/{video_id}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=8) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Tìm định dạng audio tốt nhất
                        audio_formats = [f for f in data.get('adaptiveFormats', []) 
                                        if f.get('type', '').startswith('audio/')]
                        
                        if audio_formats:
                            # Sắp xếp theo bitrate giảm dần
                            audio_formats.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                            
                            result = {
                                'title': data.get('title', 'Unknown'),
                                'url': audio_formats[0].get('url'),
                                'expires': int(time.time()) + 21600  # 6 giờ
                            }
                            
                            # Lưu vào cache
                            result['expires_at'] = result['expires']
                            invidious_cache[video_id] = result
                            
                            # Dọn dẹp cache nếu quá lớn
                            if len(invidious_cache) > 100:
                                oldest = sorted(
                                    invidious_cache.items(),
                                    key=lambda x: x[1].get('expires_at', 0)
                                )[:30]
                                for old_id, _ in oldest:
                                    invidious_cache.pop(old_id, None)
                                    
                            return result
            return None
        except Exception as e:
            print(f"Lỗi với Invidious instance {instance}: {e}")
            return None
    
    # Chạy tất cả các instance song song
    tasks = [asyncio.create_task(try_instance(instance)) for instance in INVIDIOUS_INSTANCES]
    results = await asyncio.gather(*tasks)
    
    # Lấy kết quả đầu tiên không phải None
    for result in results:
        if result:
            return result
    
    return None

async def fetch_from_ytdl_patched(video_id):
    """Sử dụng các instances ytdl-patched công khai làm proxy"""
    ytdl_patched_instances = [
        "https://co.wuk.sh",
        "https://pipedapi.kavin.rocks",
        "https://api.piped.privacydev.net",
        "https://watchapi.whatever.social"
    ]
    
    for instance in ytdl_patched_instances:
        try:
            api_url = f"{instance}/api/v1/streams/{video_id}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Lấy format audio tốt nhất
                        audio_formats = [f for f in data.get('audioStreams', []) 
                                        if f.get('quality')]
                        
                        if audio_formats:
                            # Sắp xếp theo bitrate giảm dần
                            audio_formats.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                            
                            return {
                                'title': data.get('title', 'Unknown'),
                                'url': audio_formats[0].get('url'),
                                'expires': int(time.time()) + 86400  # 24 giờ
                            }
                            
        except Exception as e:
            print(f"Lỗi với ytdl-patched instance {instance}: {e}")
            continue
    
    return None

def get_ydl_opts(playlist=False, extract_flat=False):
    """Tạo ydl_opts với headers phù hợp để tránh phát hiện bot"""
    # Tạo User-Agent ngẫu nhiên
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ]
    # Tạo chuỗi Accept Languages ngẫu nhiên
    accept_languages = ['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en;q=0.9,vi;q=0.8', 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7']
    
    # Random user agent mỗi lần request
    user_agent = random.choice(user_agents)
    accept_language = random.choice(accept_languages)
    
    # Mô phỏng trình duyệt thực
    sec_ch_ua = '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'
    sec_ch_ua_platform = random.choice(['"Windows"', '"macOS"', '"Linux"'])
    
    # Thêm request delay để tránh rate limit
    return {
        'format': 'bestaudio/best',
        'quiet': True,
        'noplaylist': not playlist,
        'extract_flat': 'in_playlist' if extract_flat else False,
        'skip_download': True,
        'nopart': True,
        'no_warnings': True,
        'ignoreerrors': True,
        'nocheckcertificate': True,
        'socket_timeout': 15,
        'retry_sleep_functions': {
            'http': lambda x: 5 * (2 ** (x - 1)) if x <= 10 else 120
        },
        'retries': 5,
        'fragment_retries': 5,
        'extractor_retries': 5,
        'geo_bypass': True,
        'geo_bypass_country': 'US',
        'sleep_interval': 5,  # Thêm delay giữa các request
        'max_sleep_interval': 10,
        'sleep_interval_requests': 2,  # Sleep sau mỗi 2 requests
        # Headers giả lập trình duyệt thực tế
        'http_headers': {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': accept_language,
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
            'Sec-CH-UA': sec_ch_ua,
            'Sec-CH-UA-Platform': sec_ch_ua_platform,
            'Referer': 'https://www.google.com/',
            'Cache-Control': 'max-age=0',
            'TE': 'trailers',
            'DNT': '1'
        }
    }

# Hàm mới để lấy stream URL trực tiếp từ Piped (rất ổn định)
async def get_stream_from_piped(video_id):
    """Lấy stream URL từ Piped API - phương pháp ổn định nhất trên Railway"""
    for instance in PIPED_INSTANCES:
        try:
            api_url = f"{instance}/streams/{video_id}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Lấy audio URL chất lượng cao nhất
                        audio_streams = data.get('audioStreams', [])
                        if audio_streams:
                            # Sắp xếp theo quality giảm dần
                            audio_streams.sort(key=lambda x: int(x.get('bitrate', 0)), reverse=True)
                            return {
                                'title': data.get('title', 'Unknown'),
                                'url': audio_streams[0].get('url'),
                                'expires': int(time.time()) + 21600  # 6 giờ
                            }
            print(f"Không tìm thấy audio streams từ {instance}")
        except Exception as e:
            print(f"Lỗi khi sử dụng Piped instance {instance}: {e}")
    
    return None

def is_url(text):
    """Phiên bản đơn giản hơn và nhanh hơn để kiểm tra URL"""
    return text.startswith(('http://', 'https://'))

# Add after your other URL detection functions
def is_spotify_url(url):
    """Check if the URL is from Spotify"""
    parsed_url = urlparse(url)
    return parsed_url.netloc in ['open.spotify.com', 'spotify.com']

def get_spotify_type_and_id(url):
    """Extract the type (track, album, playlist) and ID from a Spotify URL"""
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    
    if len(path_parts) >= 3:
        item_type = path_parts[1]  # track, album, playlist
        item_id = path_parts[2].split('?')[0]  # Remove any query parameters
        return item_type, item_id
    return None, None

# Initialize Spotify client
try:
    from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
    spotify_client = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        )
    )
    spotify_enabled = True
except (ImportError, NameError):
    spotify_enabled = False
    print("Spotify credentials not found. Spotify support disabled.")

# Add these global dictionaries after your other global dictionaries
music_queues = {}  # {guild_id: [song1, song2, ...]}
currently_playing = {}  # {guild_id: song_info}

def extract_youtube_video_id(url):
    """Extract only the video ID from a YouTube URL, removing playlist parameters"""
    # Try to parse with urllib first
    parsed_url = urlparse(url)
    
    # For standard youtube.com URLs
    if 'youtube.com' in parsed_url.netloc:
        query_params = parse_qs(parsed_url.query)
        if 'v' in query_params:
            return query_params['v'][0]
    
    # For youtu.be short URLs
    elif 'youtu.be' in parsed_url.netloc:
        return parsed_url.path.lstrip('/')
    
    # If parsing fails, try regex approach
    youtube_regex = r'(?:youtube\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|\S*?[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})'
    match = re.search(youtube_regex, url)
    
    if match:
        return match.group(1)
    
    # Return the original URL if we couldn't extract the ID
    return url

# Thêm hàm này để lưu cache stream URL vào file
def save_stream_cache():
    try:
        with open("stream_cache.json", "w", encoding="utf-8") as f:
            # Chỉ lưu các cache còn hiệu lực
            now = time.time()
            valid_cache = {
                vid: data for vid, data in stream_url_cache.items() 
                if now < data.get('expires_at', 0)
            }
            json.dump(valid_cache, f)
    except Exception as e:
        print(f"Lỗi khi lưu stream cache: {e}")

# Thêm hàm này để nạp cache từ file
def load_stream_cache():
    global stream_url_cache
    try:
        if os.path.exists("stream_cache.json"):
            with open("stream_cache.json", "r", encoding="utf-8") as f:
                stream_url_cache = json.load(f)
                
                # Lọc ra các cache còn hiệu lực
                now = time.time()
                stream_url_cache = {
                    vid: data for vid, data in stream_url_cache.items() 
                    if now < data.get('expires_at', 0)
                }
    except Exception as e:
        print(f"Lỗi khi nạp stream cache: {e}")
        stream_url_cache = {}

def is_playlist_url(url):
    """Check if the URL is a YouTube playlist"""
    parsed_url = urlparse(url)
    if 'youtube.com' in parsed_url.netloc:
        query_params = parse_qs(parsed_url.query)
        return 'list' in query_params
    return False

async def play_next_stream(guild_id, voice_client):
    if guild_id in music_queues and len(music_queues[guild_id]) > 0:
        # Lấy tối đa 3 bài kế tiếp để pre-fetch
        songs_to_prefetch = music_queues[guild_id][:3]
        
        # Tạo và chạy các task prefetch song song
        prefetch_tasks = [asyncio.create_task(prefetch_stream_url(song)) for song in songs_to_prefetch]
        
        # Kiểm tra voice client có còn kết nối không
        if not voice_client or not voice_client.is_connected():
            # Thử kết nối lại nếu có thể
            for guild in bot.guilds:
                if str(guild.id) == guild_id and guild.voice_client is None:
                    for vc in guild.voice_channels:
                        if vc.members:  # Kết nối với channel đầu tiên có người
                            try:
                                voice_client = await vc.connect()
                                break
                            except Exception as e:
                                print(f"Không thể kết nối lại: {e}")
                                return
            
            # Nếu không thể kết nối lại, dừng xử lý
            if not voice_client or not voice_client.is_connected():
                return
                
        # Lấy bài hát tiếp theo
        song_info = music_queues[guild_id].pop(0)
        currently_playing[guild_id] = song_info
        # Lưu thời gian bắt đầu phát
        currently_playing[guild_id]['start_time'] = time.time()

        try:
            is_playlist = is_playlist_url(song_info['url'])
            # Kiểm tra xem đã có stream_url chưa và còn hiệu lực không
            now = time.time()
            video_id = extract_youtube_video_id(song_info['url'])
            
            stream_url = None
            
            # Phương pháp 1: Kiểm tra cache toàn cục
            if video_id in stream_url_cache and now < stream_url_cache[video_id].get('expires_at', 0):
                stream_url = stream_url_cache[video_id]['url']
                print(f"Sử dụng URL stream từ cache toàn cục cho {video_id}")
            
            # Phương pháp 2: Sau đó kiểm tra cache trong bài hát
            elif 'stream_url' in song_info and song_info.get('stream_url') and 'expires_at' in song_info and song_info.get('expires_at', 0) > now:
                stream_url = song_info['stream_url']
                print("Sử dụng URL stream đã lưu, không cần truy vấn yt-dlp")
            
            # Phương pháp 3: Sử dụng YouTube API
            elif hasattr(bot, 'youtube_api_enabled') and bot.youtube_api_enabled and video_id:
                try:
                    result = await search_youtube_api(song_info['title'])
                    if result and 'id' in result:
                        # Tiếp tục với yt-dlp để lấy stream URL
                        ydl_opts = get_ydl_opts(is_playlist, is_playlist)
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            info = ydl.extract_info(f"https://www.youtube.com/watch?v={result['id']}", download=False)
                            if info and 'url' in info:
                                stream_url = info['url']
                                expires = info.get('expires', 3600)
                                song_info['stream_url'] = stream_url
                                song_info['expires_at'] = now + min(expires, 21600)
                except Exception as api_err:
                    print(f"YouTube API failed: {api_err}")
            
            # Phương pháp 4: Sử dụng yt-dlp
            if not stream_url:
                try:
                    ydl_opts = get_ydl_opts(is_playlist, is_playlist)
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(song_info['url'], download=False)
                        if 'entries' in info:
                            info = info['entries'][0]
                        
                        stream_url = info.get('url')
                        expires = info.get('expires', 3600)
                        
                        # Lưu lại URL stream và thời gian hết hạn
                        song_info['stream_url'] = stream_url
                        song_info['expires_at'] = now + min(expires, 21600)
                        
                        # Lưu vào cache toàn cục
                        if video_id:
                            stream_url_cache[video_id] = {
                                'url': stream_url,
                                'expires_at': song_info['expires_at']
                            }
                            
                        # Dọn dẹp cache nếu quá lớn (giới hạn 100 bài)
                        if len(stream_url_cache) > 100:
                            # Xóa 30 mục cũ nhất
                            oldest = sorted(
                                stream_url_cache.items(),
                                key=lambda x: x[1].get('expires_at', 0)
                            )[:30]
                            for old_id, _ in oldest:
                                stream_url_cache.pop(old_id, None)
                except Exception as ydl_err:
                    print(f"yt-dlp extraction failed: {ydl_err}")
            
            # Phương pháp 5: Nếu cả các phương pháp thất bại, thử Invidious
            if not stream_url and video_id:
                try:
                    invidious_result = await fetch_from_invidious(video_id)
                    if invidious_result and 'url' in invidious_result:
                        stream_url = invidious_result['url']
                        song_info['stream_url'] = stream_url
                        song_info['expires_at'] = invidious_result['expires']
                        
                        # Lưu vào cache toàn cục
                        stream_url_cache[video_id] = {
                            'url': stream_url,
                            'expires_at': invidious_result['expires']
                        }
                except Exception as inv_err:
                    print(f"Invidious extraction failed: {inv_err}")
            
            # Nếu không tìm được stream URL sau tất cả các phương pháp
            if not stream_url:
                print(f"Cannot extract stream URL for {song_info['title']} after all methods")
                # Chuyển sang bài hát tiếp theo
                await asyncio.sleep(1)
                await play_next_stream(guild_id, voice_client)
                return
            
            if not stream_url:
                # Khi không thể lấy stream URL, dùng backup station
                backup_stream = BACKUP_STREAMS.get("lofi")  # mặc định lofi
                if backup_stream:
                    song_info['stream_url'] = backup_stream
                    song_info['expires_at'] = now + 86400  # 24 giờ
                    stream_url = backup_stream
                    await channel.send("⚠️ **Không thể lấy stream từ YouTube - Đang chơi radio lofi thay thế**\nHãy thử `/playlocal` để chọn radio khác.")
                
            # Xác định việc cần làm sau khi bài hát kết thúc
            def after_playing(error):
                if error:
                    print(f"Lỗi trình phát: {error}")
                    # Kiểm tra lỗi liên quan đến kết nối
                    if "Connection reset by peer" in str(error) or "Pipe broken" in str(error):
                        try:
                            # Thử kết nối lại ở lần phát tiếp theo
                            async def reconnect_attempt():
                                nonlocal voice_client, guild_id
                                for guild in bot.guilds:
                                    if str(guild.id) == guild_id:
                                        for vc in guild.voice_channels:
                                            if vc.members:
                                                try:
                                                    voice_client = await vc.connect()
                                                    return
                                                except:
                                                    pass
                            
                            # Chạy ngoài vòng lặp chính
                            asyncio.run_coroutine_threadsafe(reconnect_attempt(), bot.loop)
                        except Exception as ex:
                            print(f"Lỗi khi thử kết nối lại: {ex}")
                
                # Bắt đầu bài hát tiếp theo
                asyncio.run_coroutine_threadsafe(play_next_stream(guild_id, voice_client), bot.loop)
            
            audio_source = discord.FFmpegPCMAudio(stream_url, **FFMPEG_OPTIONS)
            audio_source = discord.PCMVolumeTransformer(audio_source, volume=0.5)     
            # Phát âm thanh
            voice_client.play(audio_source, after=after_playing)
            
            # Tiền trích xuất URL stream cho bài hát tiếp theo
            if guild_id in music_queues and len(music_queues[guild_id]) > 0:
                next_song = music_queues[guild_id][0]
                asyncio.create_task(prefetch_stream_url(next_song))

            # Gửi thông báo đến kênh với embed thay vì text đơn giản
            if voice_client.channel:
                channel = bot.get_channel(voice_client.channel.id)
                if channel:
                    # Tạo embed cho bài hát đang phát
                    embed = discord.Embed(
                        title="🎵 Đang phát",
                        description=f"**{song_info['title']}**",
                        color=0x3498db
                    )
                    embed.add_field(
                        name="Yêu cầu bởi", 
                        value=song_info['requester'],
                        inline=True
                    )
                    
                    # Thêm thông tin về hàng đợi
                    queue_length = len(music_queues.get(guild_id, []))
                    if queue_length > 0:
                        embed.add_field(
                            name="Hàng đợi",
                            value=f"{queue_length} bài hát tiếp theo",
                            inline=True
                        )
                    
                    embed.set_footer(text="Sử dụng /music để hiển thị bảng điều khiển nhạc")
                    
                    asyncio.run_coroutine_threadsafe(
                        channel.send(embed=embed),
                        bot.loop
                    )
        except Exception as e:
            print(f"Lỗi khi phát bài hát: {str(e)}")
            # Chuyển sang bài hát tiếp theo nếu có lỗi
            if guild_id in music_queues and music_queues[guild_id]:
                await asyncio.sleep(1)  # Đợi một chút trước khi thử lại
                await play_next_stream(guild_id, voice_client)
            else:
                if guild_id in currently_playing:
                    currently_playing.pop(guild_id, None)

@bot.tree.command(name="play", description="Phát nhạc từ YouTube hoặc Spotify (hỗ trợ video, playlist, album)")
async def play(interaction: discord.Interaction, url: str):
    # Ngay lập tức phản hồi để giữ tương tác còn sống
    await interaction.response.defer(ephemeral=False, thinking=True)
    
    # Kiểm tra người dùng có ở trong voice channel không
    if interaction.user.voice is None:
        await interaction.followup.send("Cậu cần ở trong voice channel để Shore phát nhạc.")
        return
        
    voice_channel = interaction.user.voice.channel
    guild_id = str(interaction.guild.id)
    
    # Kết nối tới voice channel nếu chưa kết nối
    if interaction.guild.voice_client is None:
        try:
            voice_client = await voice_channel.connect()
        except Exception as e:
            await interaction.followup.send(f"Không thể kết nối voice channel: {str(e)}")
            return
    else:
        voice_client = interaction.guild.voice_client
        
    # Kiểm tra channel có phải là cùng một channel không
    if voice_client.channel != voice_channel:
        await interaction.followup.send(f"Shore đang ở trong voice channel khác: {voice_client.channel.mention}")
        return
        
    # Khởi tạo hàng đợi cho guild này nếu chưa tồn tại
    if guild_id not in music_queues:
        music_queues[guild_id] = []
    
    # Giới hạn kích thước hàng đợi để tránh vấn đề về bộ nhớ
    if len(music_queues[guild_id]) >= 100:  # Tăng giới hạn để hỗ trợ playlist
        await interaction.response.send_message("Hàng đợi đã đầy (tối đa 100 bài). Vui lòng đợi một số bài hát kết thúc.", ephemeral=True)
        return
    
    # Kiểm tra xem input có phải là URL không
    if not is_url(url):
        # Nếu không phải URL, xử lý như một truy vấn tìm kiếm
        await process_youtube_search(interaction, url, guild_id, voice_client)
        return
    
    # Kiểm tra xem URL có phải là Spotify không
    if spotify_enabled and is_spotify_url(url):
        try:
            item_type, item_id = get_spotify_type_and_id(url)
            
            if not item_type or not item_id:
                await interaction.followup.send("❌ Không thể xác định loại URL Spotify.", ephemeral=False)
                return
                
            # Thêm sau đoạn sau trong phần xử lý track Spotify (sau dòng "Tìm kiếm trên YouTube")
            if item_type == "track":
                # Bài hát đơn lẻ
                track = spotify_client.track(item_id)
                track_name = f"{track['artists'][0]['name']} - {track['name']}"
                
                # Tìm kiếm trên YouTube
                search_query = f"{track_name} audio"
                if not await process_youtube_search(interaction, search_query, guild_id, voice_client):
                    # Thêm thông báo lỗi nếu không tìm thấy
                    await interaction.followup.send(
                        f"❌ Không tìm thấy **{track_name}** trên YouTube. Vui lòng:\n"
                        f"- Thử tìm bài hát này trực tiếp trên YouTube và sử dụng link YouTube\n"
                        f"- Tìm một phiên bản khác của bài hát\n"
                        f"- Dùng lệnh `/play {track_name}` để tìm thủ công",
                        ephemeral=False
                    )
                
            elif item_type == "album":
                # Album
                album = spotify_client.album(item_id)
                album_name = album['name']
                artist_name = album['artists'][0]['name']
                tracks = spotify_client.album_tracks(item_id)['items']
                
                added_count = 0
                for track in tracks[:25]:  # Giới hạn 25 bài
                    track_name = f"{artist_name} - {track['name']}"
                    
                    # Thêm bài hát vào hàng đợi mà không thông báo từng bài
                    if await queue_from_search(track_name, interaction.user.name, guild_id):
                        added_count += 1
                
                # Bắt đầu phát nếu chưa phát
                if not voice_client.is_playing():
                    await play_next_stream(guild_id, voice_client)
                
                await interaction.followup.send(
                    f"📋 Đã thêm **{added_count}** bài hát từ album **{album_name}** vào hàng đợi.",
                    ephemeral=False
                )
                
            elif item_type == "playlist":
                # Playlist
                playlist = spotify_client.playlist(item_id)
                playlist_name = playlist['name']
                tracks = spotify_client.playlist_tracks(item_id)['items']
                
                added_count = 0
                # Tạo danh sách các task
                tasks = []
                for item in tracks[:25]:  # Giới hạn 25 bài
                    track = item['track']
                    if track:
                        artist_name = track['artists'][0]['name'] if track['artists'] else "Unknown"
                        track_name = f"{artist_name} - {track['name']}"
                        
                        # Tạo task thay vì đợi lần lượt
                        task = asyncio.create_task(queue_from_search(track_name, interaction.user.name, guild_id))
                        tasks.append(task)
                
                # Chờ tất cả các task hoàn thành cùng lúc
                results = await asyncio.gather(*tasks)
                
                # Đếm số bài đã thêm thành công
                added_count = sum(1 for result in results if result)
                
                # Bắt đầu phát nếu chưa phát
                if not voice_client.is_playing():
                    await play_next_stream(guild_id, voice_client)
                
                await interaction.followup.send(
                    f"📋 Đã thêm **{added_count}** bài hát từ playlist **{playlist_name}** vào hàng đợi.",
                    ephemeral=False
                )
                
                # Bắt đầu phát nếu chưa phát
                if not voice_client.is_playing():
                    await play_next_stream(guild_id, voice_client)
                
                await interaction.followup.send(
                    f"📋 Đã thêm **{added_count}** bài hát từ playlist **{playlist_name}** vào hàng đợi.",
                    ephemeral=False
                )
            else:
                await interaction.followup.send(f"❌ Loại Spotify '{item_type}' không được hỗ trợ.", ephemeral=False)
                
        except Exception as e:
            await interaction.followup.send(f"❌ Lỗi khi xử lý Spotify: {str(e)}", ephemeral=False)
            return
    else:
        # Xử lý URL YouTube
        is_playlist = is_playlist_url(url)
        
        # Thiết lập các tùy chọn yt-dlp dựa trên loại URL
        ydl_opts = get_ydl_opts(is_playlist, is_playlist)
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                # Thêm kiểm tra này để tránh lỗi NoneType
                if info is None:
                    await interaction.followup.send(f"❌ Không thể lấy thông tin video. URL không hợp lệ hoặc bị chặn bởi YouTube.", ephemeral=False)
                    return
                
                # Thay đổi trong phần xử lý YouTube playlist
                if is_playlist and 'entries' in info:
                    # Đây là playlist, xử lý tất cả các video
                    added_count = 0
                    playlist_title = info.get('title', 'Unknown Playlist')
                    
                    # Giới hạn số lượng bài hát thêm vào
                    max_songs = min(25, len(info['entries']))
                    entries = [entry for entry in info['entries'][:max_songs] if entry]
                    
                    # Tạo task xử lý song song
                    async def process_entry(entry):
                        song_info = {
                            'title': entry.get('title', 'Unknown'),
                            'url': f"https://www.youtube.com/watch?v={entry['id']}",
                            'requester': interaction.user.name
                        }
                        music_queues[guild_id].append(song_info)
                        return True
                    
                    # Tạo và chờ tất cả các task hoàn thành
                    tasks = [asyncio.create_task(process_entry(entry)) for entry in entries]
                    results = await asyncio.gather(*tasks)
                    
                    added_count = sum(results)
                    
                    await interaction.followup.send(f"📋 Đã thêm **{added_count}** bài hát từ playlist **{playlist_title}** vào hàng đợi.", ephemeral=False)
                else:
                    # Đây là video đơn lẻ
                    if 'entries' in info:
                        # Đó là kết quả tìm kiếm
                        info = info['entries'][0]
                    
                    video_title = info.get('title', 'Unknown')
                    
                    # Thêm vào hàng đợi - lưu URL thay vì file
                    song_info = {
                        'title': video_title,
                        'url': url if not 'id' in info else f"https://www.youtube.com/watch?v={info['id']}",
                        'requester': interaction.user.name
                    }
                    
                    music_queues[guild_id].append(song_info)
                    
                    await interaction.followup.send(f"🎵 Đã thêm **{video_title}** vào hàng đợi phát nhạc.", ephemeral=False)
                
                # Bắt đầu phát nếu chưa phát
                if not voice_client.is_playing():
                    await play_next_stream(guild_id, voice_client)
                    
        except Exception as e:
            await interaction.followup.send(f"❌ Lỗi khi tìm thông tin bài hát: {str(e)}", ephemeral=False)
            return

async def queue_from_search(search_query, requester, guild_id):
    try:
        # Tạo danh sách các biến thể tìm kiếm
        search_queries = [
            search_query,  # Query gốc
            f"{search_query} audio",
            f"{search_query} official"
        ]
        
        # Tạo hàm tìm kiếm cho một query
        async def search_single_query(query):
            try:
                ydl_opts = {
                    # các option hiện tại
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"ytsearch:{query}", download=False)
                    if 'entries' in info and info['entries']:
                        return {
                            'title': info['entries'][0].get('title', 'Unknown'),
                            'url': f"https://www.youtube.com/watch?v={info['entries'][0]['id']}",
                            'query': query
                        }
                return None
            except:
                return None
        
        # Chạy tất cả các query song song và lấy kết quả đầu tiên thành công
        search_tasks = [asyncio.create_task(search_single_query(q)) for q in search_queries]
        results = await asyncio.gather(*search_tasks)
        
        # Lấy kết quả đầu tiên không phải None
        for result in results:
            if result:
                # Thêm vào hàng đợi
                song_info = {
                    'title': result['title'],
                    'url': result['url'],
                    'requester': requester
                }
                
                if guild_id not in music_queues:
                    music_queues[guild_id] = []
                
                music_queues[guild_id].append(song_info)
                return True
                
        return False
    except Exception as e:
        print(f"Error queueing from search: {e}")
        return False

class SongInfo:
    def __init__(self, url, title, requester, stream_url=None, expires_at=None):
        self.url = url
        self.title = title
        self.requester = requester
        self.stream_url = stream_url  # Lưu URL stream để tái sử dụng
        self.expires_at = expires_at 

# Thêm cache cho kết quả tìm kiếm
search_cache = {}  # {query: results}
cache_expiry = {}  # {query: expiry_time}
CACHE_DURATION = 3600  # 1 giờ

async def search_youtube_api(query):
    """Tìm kiếm video trên YouTube sử dụng YouTube Data API"""
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        
        # Thực hiện tìm kiếm
        request = youtube.search().list(
            part="snippet",
            maxResults=1,
            q=query,
            type="video"
        )
        response = request.execute()
        
        # Kiểm tra kết quả
        if 'items' in response and response['items']:
            video = response['items'][0]
            video_id = video['id']['videoId']
            video_title = video['snippet']['title']
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            return {
                'title': video_title,
                'url': video_url,
                'id': video_id
            }
    except HttpError as e:
        print(f"YouTube API error: {e}")
    except Exception as e:
        print(f"Error searching YouTube API: {e}")
    
    return None

async def process_youtube_search(interaction, query, guild_id, voice_client):
    now = time.time()
    
    try:
        # Kiểm tra cache
        if query in search_cache and now < cache_expiry.get(query, 0):
            # Sử dụng kết quả từ cache
            video_title = search_cache[query]["title"]
            video_url = search_cache[query]["url"]
        else:
            # Thử dùng YouTube API trước
            result = await search_youtube_api(query)
            
            if result:
                video_title = result['title']
                video_url = result['url']
                
                # Lưu vào cache
                search_cache[query] = {"title": video_title, "url": video_url}
                cache_expiry[query] = now + CACHE_DURATION
            else:
                # Nếu API thất bại, thử với yt-dlp như backup
                try:
                    ydl_opts = get_ydl_opts()
                    ydl_opts['default_search'] = 'ytsearch1'
                    
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(f"ytsearch:{query}", download=False)
                        
                        if not info:
                            raise Exception("Không nhận được thông tin tìm kiếm")
                        
                        if 'entries' not in info or not info['entries']:
                            raise Exception("Không tìm thấy kết quả")
                        
                        video = info['entries'][0]
                        if not video:
                            raise Exception("Không thể xử lý kết quả tìm kiếm")
                        
                        video_title = video.get('title', 'Unknown')
                        video_id = video.get('id')
                        
                        if not video_id:
                            raise Exception("Không tìm thấy ID video")
                            
                        video_url = f"https://www.youtube.com/watch?v={video_id}"
                        
                        # Lưu vào cache
                        search_cache[query] = {"title": video_title, "url": video_url}
                        cache_expiry[query] = now + CACHE_DURATION
                except Exception as e:
                    # Nếu cả hai phương pháp đều thất bại
                    await interaction.followup.send(f"❌ Không thể tìm kiếm '{query}': {str(e)}", ephemeral=False)
                    return False
        
        # Thêm vào hàng đợi
        song_info = {
            'title': video_title,
            'url': video_url,
            'requester': interaction.user.name
        }
        
        if guild_id not in music_queues:
            music_queues[guild_id] = []
        
        music_queues[guild_id].append(song_info)
        
        await interaction.followup.send(
            f"🎵 Đã thêm **{video_title}** vào hàng đợi phát nhạc.", 
            ephemeral=False
        )
        
        # Bắt đầu phát nếu chưa phát
        if not voice_client.is_playing():
            await play_next_stream(guild_id, voice_client)
        
        return True
        
    except Exception as e:
        print(f"Search error: {str(e)}")
        await interaction.followup.send(f"❌ Lỗi khi tìm kiếm: {str(e)}", ephemeral=False)
        return False

@bot.tree.command(name="volume", description="Điều chỉnh âm lượng phát nhạc (0-100)")
async def volume(interaction: discord.Interaction, level: int):
    """Điều chỉnh âm lượng phát nhạc"""
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    
    # Kiểm tra bot có đang ở trong voice channel không
    if not voice_client or not voice_client.is_connected():
        await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
        return
    
    # Kiểm tra giá trị âm lượng hợp lệ
    if level < 0 or level > 100:
        await interaction.response.send_message("Âm lượng phải nằm trong khoảng 0-100.", ephemeral=True)
        return
    
    # Điều chỉnh âm lượng (0.0 - 1.0)
    voice_client.source.volume = level / 100.0
    
    # Tạo embed thông tin
    embed = discord.Embed(
        title="🔊 Đã điều chỉnh âm lượng",
        description=f"Âm lượng hiện tại: **{level}%**",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Tạo thanh hiển thị âm lượng trực quan
    progress = int(level / 10)
    volume_bar = "🔊 " + "█" * progress + "░" * (10 - progress)
    embed.add_field(name="Mức âm lượng", value=volume_bar, inline=False)
    
    # Thêm nút điều chỉnh nhanh
    class VolumeView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            
        @discord.ui.button(label="🔈 0%", style=discord.ButtonStyle.secondary)
        async def volume_0(self, vol_interaction: discord.Interaction, button: discord.ui.Button):
            voice_client.source.volume = 0.0
            await vol_interaction.response.send_message("🔇 Đã tắt âm", ephemeral=True)
            
        @discord.ui.button(label="🔉 50%", style=discord.ButtonStyle.secondary)
        async def volume_50(self, vol_interaction: discord.Interaction, button: discord.ui.Button):
            voice_client.source.volume = 0.5
            await vol_interaction.response.send_message("🔉 Âm lượng: 50%", ephemeral=True)
            
        @discord.ui.button(label="🔊 100%", style=discord.ButtonStyle.secondary)
        async def volume_100(self, vol_interaction: discord.Interaction, button: discord.ui.Button):
            voice_client.source.volume = 1.0
            await vol_interaction.response.send_message("🔊 Âm lượng: 100%", ephemeral=True)
    
    await interaction.response.send_message(embed=embed, view=VolumeView(), ephemeral=False)
    
@bot.tree.command(name="find", description="Tìm bài hát trong hàng đợi")
async def find(interaction: discord.Interaction, keyword: str):
    """Tìm kiếm bài hát trong hàng đợi hiện tại"""
    await interaction.response.defer(ephemeral=False, thinking=True)
    guild_id = str(interaction.guild.id)
    
    # Kiểm tra có hàng đợi không
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.followup.send("Hàng đợi nhạc đang trống.", ephemeral=False)
        return
    
    # Tìm kiếm trong hàng đợi
    keyword = keyword.lower()
    matches = []
    
    # Tìm trong bài hát đang phát
    current = currently_playing.get(guild_id)
    if current and keyword in current['title'].lower():
        matches.append((-1, current))  # Dùng vị trí -1 cho bài đang phát
    
    # Tìm trong hàng đợi
    for i, song in enumerate(music_queues[guild_id]):
        if keyword in song['title'].lower():
            matches.append((i, song))
    
    # Tạo embed kết quả
    embed = discord.Embed(
        title=f"🔍 Kết quả tìm kiếm: '{keyword}'",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    if not matches:
        embed.description = f"Không tìm thấy bài hát nào khớp với từ khóa '{keyword}'"
    else:
        embed.description = f"Tìm thấy {len(matches)} kết quả khớp với từ khóa '{keyword}'"
        
        # Hiển thị kết quả tìm kiếm
        results = ""
        for pos, song in matches[:10]:  # Giới hạn 10 kết quả đầu tiên
            if pos == -1:
                results += f"▶️ **Đang phát**: {song['title']}\n"
            else:
                results += f"#{pos+1}: **{song['title']}**\n"
        
        if len(matches) > 10:
            results += f"\n*...và {len(matches) - 10} kết quả khác*"
            
        embed.add_field(name="Các bài hát khớp từ khóa", value=results, inline=False)
        
        # Thêm thông tin điều hướng
        if len(matches) > 0:
            embed.set_footer(text="Sử dụng /jump <số thứ tự> để chuyển đến bài hát cụ thể")
    
    # Thêm menu điều hướng
    class SearchResultView(discord.ui.View):
        def __init__(self, matches):
            super().__init__(timeout=60)
            self.matches = matches
            
            # Chỉ hiển thị nút nếu có kết quả trong hàng đợi
            queue_matches = [m for m in matches if m[0] >= 0]
            if not queue_matches:
                for child in self.children:
                    child.disabled = True
        
        @discord.ui.select(
            placeholder="Di chuyển đến bài hát...",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=f"{i+1}. {song['title'][:70]}", 
                    value=str(i)
                ) for i, song in matches if i >= 0
            ][:25]  # Giới hạn 25 tùy chọn
        )
        async def select_song(self, select_interaction: discord.Interaction, select: discord.ui.Select):
            position = int(select.values[0]) + 1  # +1 vì vị trí hiển thị bắt đầu từ 1
            
            # Gọi lệnh jump
            voice_client = select_interaction.guild.voice_client
            if voice_client and voice_client.is_connected():
                selected_song = music_queues[guild_id].pop(position - 1)
                music_queues[guild_id].insert(0, selected_song)
                voice_client.stop()
                
                await select_interaction.response.send_message(
                    f"⏩ Đã nhảy đến bài hát **{selected_song['title']}**", 
                    ephemeral=False
                )
            else:
                await select_interaction.response.send_message(
                    "Shore không ở trong voice channel nào.", 
                    ephemeral=True
                )
    
    # Chỉ tạo view nếu có kết quả
    view = SearchResultView(matches) if matches else None
    await interaction.followup.send(embed=embed, view=view, ephemeral=False)

async def prefetch_stream_url(song_info):
    """Trích xuất URL stream cho bài hát - phiên bản ổn định cho Railway"""
    max_retries = 3
    
    # Nếu đã có stream_url và còn hiệu lực
    if 'stream_url' in song_info and song_info.get('stream_url') and 'expires_at' in song_info and song_info.get('expires_at', 0) > time.time():
        return
    
    # Lấy video ID
    video_id = extract_youtube_video_id(song_info['url'])
    if not video_id:
        print(f"Không thể lấy video ID từ {song_info['url']}")
        return
    
    # Kiểm tra cache toàn cục trước
    now = time.time()
    if video_id in stream_url_cache and now < stream_url_cache[video_id].get('expires_at', 0):
        song_info['stream_url'] = stream_url_cache[video_id]['url']
        song_info['expires_at'] = stream_url_cache[video_id]['expires_at']
        print(f"Sử dụng URL stream từ cache toàn cục cho {video_id}")
        return
    
    # Thứ tự ưu tiên cho Railway: Piped > Invidious > ytdl-patched > yt-dlp
    for attempt in range(max_retries):
        # 1. Thử Piped API - phương pháp ổn định nhất
        try:
            piped_result = await get_stream_from_piped(video_id)
            if piped_result and 'url' in piped_result:
                song_info['stream_url'] = piped_result['url']
                song_info['expires_at'] = piped_result['expires']
                
                # Lưu cache
                stream_url_cache[video_id] = {
                    'url': piped_result['url'],
                    'expires_at': piped_result['expires']
                }
                print(f"✅ Đã lấy URL từ Piped cho {video_id}")
                return
        except Exception as e:
            print(f"❌ Lỗi Piped: {e}")
        
        # 2. Thử Invidious
        try:
            invidious_result = await fetch_from_invidious(video_id)
            if invidious_result and 'url' in invidious_result:
                song_info['stream_url'] = invidious_result['url']
                song_info['expires_at'] = invidious_result['expires']
                
                # Lưu cache
                stream_url_cache[video_id] = {
                    'url': invidious_result['url'],
                    'expires_at': invidious_result['expires']
                }
                print(f"✅ Đã lấy URL từ Invidious cho {video_id}")
                return
        except Exception as e:
            print(f"❌ Lỗi Invidious: {e}")
        
        # 3. Thử ytdl-patched
        try:
            ytdlp_result = await fetch_from_ytdl_patched(video_id)
            if ytdlp_result and 'url' in ytdlp_result:
                song_info['stream_url'] = ytdlp_result['url']
                song_info['expires_at'] = ytdlp_result['expires']
                
                # Lưu cache
                stream_url_cache[video_id] = {
                    'url': ytdlp_result['url'],
                    'expires_at': ytdlp_result['expires']
                }
                print(f"✅ Đã lấy URL từ ytdl-patched cho {video_id}")
                return
        except Exception as e:
            print(f"❌ Lỗi ytdl-patched: {e}")
        
        # 4. Thử yt-dlp với timeout (ít thành công nhất trên Railway)
        if attempt == max_retries - 1:  # Chỉ thử ở lần retry cuối
            try:
                ydl_opts = get_ydl_opts()
                async def extract_with_timeout():
                    try:
                        loop = asyncio.get_event_loop()
                        return await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(song_info['url'], download=False))
                    except Exception as e:
                        print(f"❌ Lỗi trích xuất yt-dlp: {e}")
                        return None
                
                info = await asyncio.wait_for(extract_with_timeout(), timeout=15)
                if info and 'url' in info:
                    song_info['stream_url'] = info['url']
                    expires = info.get('expires', 3600)
                    song_info['expires_at'] = now + min(expires, 21600)
                    
                    # Lưu cache
                    stream_url_cache[video_id] = {
                        'url': song_info['stream_url'],
                        'expires_at': song_info['expires_at']
                    }
                    print(f"✅ Đã lấy URL từ yt-dlp cho {video_id}")
                    return
            except Exception as e:
                print(f"❌ Lỗi yt-dlp: {e}")
        
        # Nếu tất cả đều thất bại, đợi một chút và thử lại
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 2
            print(f"⏳ Thử lại sau {wait_time}s (lần {attempt+1}/{max_retries})")
            await asyncio.sleep(wait_time)
    
    print(f"⛔ Không thể lấy stream URL sau {max_retries} lần thử")

@bot.tree.command(name="skip", description="Bỏ qua bài hát hiện tại")
async def skip(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    
    if voice_client is None or not voice_client.is_connected():
        await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
        return
        
    if not voice_client.is_playing():
        await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)
        return
    
    # Lấy thông tin bài hát hiện tại trước khi skip
    current_song_title = None
    if guild_id in currently_playing and 'title' in currently_playing[guild_id]:
        current_song_title = currently_playing[guild_id]['title']
    
    # Stop the current song (this will trigger the after_playing callback)
    voice_client.stop()
    
    # Phản hồi tương tác để tránh lỗi "Interaction Failed"
    if current_song_title:
        await interaction.response.send_message(f"⏭️ Đã bỏ qua bài hát **{current_song_title}**", ephemeral=False)
    else:
        await interaction.response.send_message("⏭️ Đã bỏ qua bài hát hiện tại", ephemeral=False)

@bot.tree.command(name="queue", description="Hiển thị danh sách bài hát trong hàng đợi")
async def queue(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    # Create an embed for the queue
    embed = discord.Embed(
        title="🎵 Hàng đợi nhạc",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Add currently playing song if any
    current = currently_playing.get(guild_id)
    if current:
        # Giới hạn độ dài tiêu đề bài hát để tránh lỗi
        title = current['title']
        if len(title) > 50:
            title = title[:47] + "..."
            
        embed.add_field(
            name="💿 Đang phát",
            value=f"**{title}**\nYêu cầu bởi: {current['requester']}",
            inline=False
        )
        
    max_items = 5
    
    # Check if queue is empty
    if guild_id not in music_queues or not music_queues[guild_id]:
        if current:
            embed.add_field(
                name="Hàng đợi",
                value="*Không có bài hát nào trong hàng đợi*",
                inline=False
            )
        else:
            embed.description = "*Không có bài hát nào đang phát hoặc trong hàng đợi*"
    else:
        # Format queue items
        queue_text = ""
        max_chars = 900  # Giữ giới hạn 900 ký tự để dự phòng
        total_songs = len(music_queues[guild_id])
        shown_songs = 0
        
        for i, song in enumerate(music_queues[guild_id], 1):
            # Giới hạn độ dài tiêu đề bài hát
            title = song['title']
            if len(title) > 50:
                title = title[:47] + "..."
                
            song_text = f"{i}. **{title}**\n└ Yêu cầu bởi: {song['requester']}\n\n"
            
            # Kiểm tra nếu thêm bài này có vượt quá giới hạn không
            if len(queue_text) + len(song_text) > max_chars:
                break
                
            queue_text += song_text
            shown_songs += 1
            
            # Chỉ hiển thị tối đa 10 bài
            if shown_songs >= 10:
                break
        
        # Thông báo số bài còn lại không hiển thị
        remaining = total_songs - shown_songs
        if remaining > 0:
            queue_text += f"*...và {remaining} bài hát khác*"
            
        embed.add_field(
            name=f"Hàng đợi ({total_songs} bài)",
            value=queue_text,
            inline=False
        )
    
    # Send the embed
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name="playlocal", description="Phát nhạc từ các nguồn có sẵn (không cần download)")
async def playlocal(interaction: discord.Interaction, song: str):
    """Phát nhạc từ danh sách các nguồn có sẵn, không cần download từ YouTube"""
    # Tạo một danh sách các URL trực tiếp luôn phát được
    preset_songs = {
        "lofi": "https://stream.0x.no/lofi",
        "jazz": "https://stream.0x.no/jazz",
        "chill": "https://stream.0x.no/chill",
        "classical": "https://ice1.somafm.com/classical-128-mp3",
        "rock": "https://ice1.somafm.com/groovesalad-128-mp3",
        "piano": "https://ice1.somafm.com/beatblender-128-mp3"
    }
    
    await interaction.response.defer(ephemeral=False, thinking=True)
    
    if song.lower() not in preset_songs:
        songs_list = ", ".join(f"`{name}`" for name in preset_songs.keys())
        await interaction.followup.send(f"⚠️ Bài hát không có trong danh sách. Hãy chọn một trong các bài sau: {songs_list}")
        return
    
    # Tương tự như lệnh play hiện tại, nhưng sử dụng URL trực tiếp
    if interaction.user.voice is None:
        await interaction.followup.send("Cậu cần ở trong voice channel để Shore phát nhạc.")
        return
        
    voice_channel = interaction.user.voice.channel
    guild_id = str(interaction.guild.id)
    
    # Kết nối tới voice channel
    if interaction.guild.voice_client is None:
        try:
            voice_client = await voice_channel.connect()
        except Exception as e:
            await interaction.followup.send(f"Không thể kết nối voice channel: {str(e)}")
            return
    else:
        voice_client = interaction.guild.voice_client
    
    # Phát nhạc trực tiếp
    stream_url = preset_songs[song.lower()]
    song_title = song.capitalize()
    
    # Thêm vào hàng đợi
    song_info = {
        'title': f"{song_title} Radio",
        'url': stream_url,
        'requester': interaction.user.name,
        'stream_url': stream_url,  # Stream URL đã có sẵn
        'expires_at': time.time() + 86400  # 24 giờ
    }
    
    if guild_id not in music_queues:
        music_queues[guild_id] = []
    
    music_queues[guild_id].insert(0, song_info)
    
    # Dừng bài hiện tại để chuyển sang bài mới
    if voice_client.is_playing():
        voice_client.stop()
    else:
        await play_next_stream(guild_id, voice_client)
    
    await interaction.followup.send(f"🎵 Đang phát **{song_title} Radio**")

@bot.tree.command(name="stop", description="Dừng phát nhạc và xóa danh sách chờ")
async def stop(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    
    if voice_client is None or not voice_client.is_connected():
        await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
        return
    
    # Clear the queue
    if guild_id in music_queues:
        music_queues[guild_id] = []
    
    # Stop playing
    if voice_client.is_playing():
        voice_client.stop()
    
    # Remove currently playing
    currently_playing.pop(guild_id, None)
    
    await interaction.response.send_message("⏹️ Đã dừng phát nhạc và xóa hàng đợi.", ephemeral=False)

# Music controller class
# Music controller class
class MusicControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # Persistent view
    
    @discord.ui.button(label="⏯️ Tạm dừng/Tiếp tục", style=discord.ButtonStyle.primary, custom_id="music:pause_resume")
    async def pause_resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        voice_client = interaction.guild.voice_client
        
        if voice_client is None or not voice_client.is_connected():
            await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
            return
            
        if voice_client.is_playing():
            voice_client.pause()
            await interaction.response.send_message("⏸️ Đã tạm dừng phát nhạc.", ephemeral=True)
        elif voice_client.is_paused():
            voice_client.resume()
            await interaction.response.send_message("▶️ Đã tiếp tục phát nhạc.", ephemeral=True)
        else:
            await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)
    
    @discord.ui.button(label="⏭️ Bỏ qua", style=discord.ButtonStyle.primary, custom_id="music:skip")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        voice_client = interaction.guild.voice_client
        
        if voice_client is None or not voice_client.is_connected():
            await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
            return
            
        if not voice_client.is_playing() and not voice_client.is_paused():
            await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)
            return
            
        voice_client.stop()
        await interaction.response.send_message("⏭️ Đã bỏ qua bài hát hiện tại.", ephemeral=True)
    
    @discord.ui.button(label="🔀 Xáo trộn", style=discord.ButtonStyle.primary, custom_id="music:shuffle")
    async def shuffle(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        
        if guild_id not in music_queues or not music_queues[guild_id]:
            await interaction.response.send_message("Hàng đợi nhạc đang trống.", ephemeral=True)
            return
        
        # Save queue length for the response message
        queue_length = len(music_queues[guild_id])
        
        # Randomize the order of songs in the queue
        import random
        random.shuffle(music_queues[guild_id])
        
        await interaction.response.send_message(f"🔀 Đã xáo trộn {queue_length} bài hát trong hàng đợi.", ephemeral=True)
    
    @discord.ui.button(label="⏹️ Dừng", style=discord.ButtonStyle.danger, custom_id="music:stop")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = str(interaction.guild.id)
        voice_client = interaction.guild.voice_client
        
        if voice_client is None or not voice_client.is_connected():
            await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
            return
        
        # Clear the queue
        if guild_id in music_queues:
            music_queues[guild_id] = []
        
        # Stop playing
        if voice_client.is_playing() or voice_client.is_paused():
            voice_client.stop()
        
        # Remove currently playing
        currently_playing.pop(guild_id, None)
        
        await interaction.response.send_message("⏹️ Đã dừng phát nhạc và xóa hàng đợi.", ephemeral=True)

@bot.tree.command(name="music", description="Hiển thị bảng điều khiển nhạc")
async def music_panel(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    # Create an embed for the music panel
    embed = discord.Embed(
        title="🎵 Bảng điều khiển nhạc",
        description="Sử dụng các nút bên dưới để điều khiển trình phát nhạc",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Add currently playing song if any
    current = currently_playing.get(guild_id)
    if current:
        embed.add_field(
            name="💿 Đang phát",
            value=f"**{current['title']}**\nYêu cầu bởi: {current['requester']}",
            inline=False
        )
    else:
        embed.add_field(
            name="💿 Đang phát",
            value="*Không có bài hát nào đang phát*",
            inline=False
        )
    
    # Queue summary
    queue_count = len(music_queues.get(guild_id, []))
    embed.add_field(
        name="📋 Hàng đợi",
        value=f"*{queue_count} bài hát trong hàng đợi*\nSử dụng lệnh `/queue` để xem chi tiết hoặc `/shuffle` để xáo trộn.",
        inline=False
    )
    
    # Add footer with instructions
    embed.set_footer(text="Sử dụng /play [url] để thêm nhạc vào hàng đợi")
    
    # Create the view with the buttons
    view = MusicControlView()
    
    # Send the embed with the view
    await interaction.response.send_message(embed=embed, view=view, ephemeral=False)

@bot.tree.command(name="shuffle", description="Xáo trộn thứ tự các bài hát trong hàng đợi")
async def shuffle(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.response.send_message("Hàng đợi nhạc đang trống.", ephemeral=True)
        return

    # Save queue length for the response message
    queue_length = len(music_queues[guild_id])
    
    # Randomize the order of songs in the queue
    import random
    random.shuffle(music_queues[guild_id])
    
    # Create an embed for the response
    embed = discord.Embed(
        title="🔀 Đã xáo trộn hàng đợi",
        description=f"Đã xáo trộn ngẫu nhiên {queue_length} bài hát trong hàng đợi.",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Add a field showing the first few songs in the new order
    if queue_length > 0:
        preview = "\n".join([f"{i+1}. **{song['title']}**" for i, song in enumerate(music_queues[guild_id][:5])])
        if queue_length > 5:
            preview += f"\n*...và {queue_length - 5} bài hát khác*"
        embed.add_field(name="Thứ tự mới", value=preview, inline=False)
    
    # Send the response
    await interaction.response.send_message(embed=embed, ephemeral=False)
    
@bot.tree.command(name="jump", description="Nhảy đến vị trí bài hát cụ thể trong hàng đợi")
async def jump(interaction: discord.Interaction, position: int):
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    
    # Phản hồi ngay để tránh timeout
    await interaction.response.defer(ephemeral=False, thinking=True)
    
    # Check if bot is in voice channel
    if voice_client is None or not voice_client.is_connected():
        await interaction.followup.send("Shore không ở trong voice channel nào.")
        return
    
    # Check if queue exists and has songs
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.followup.send("Hàng đợi nhạc đang trống.")
        return
    
    # Check if position is valid
    if position < 1 or position > len(music_queues[guild_id]):
        await interaction.followup.send(
            f"Vị trí không hợp lệ. Hãy chọn số từ 1 đến {len(music_queues[guild_id])}."
        )
        return
    
    # Get the song at the specified position
    position = position - 1  # Convert to 0-based index
    selected_song = music_queues[guild_id].pop(position)
    
    # Move song to the front of the queue
    music_queues[guild_id].insert(0, selected_song)
    
    # Stop current playback to trigger the next song
    voice_client.stop()
    
    await interaction.followup.send(
        f"⏩ Đã nhảy đến bài hát **{selected_song['title']}**"
    )

@bot.tree.command(name="savequeue", description="Lưu hàng đợi nhạc hiện tại để sử dụng sau")
async def savequeue(interaction: discord.Interaction, playlist_name: str):
    guild_id = str(interaction.guild.id)
    
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.response.send_message("Không có bài hát nào trong hàng đợi để lưu", ephemeral=True)
        return
        
    # Tạo thư mục playlists nếu chưa tồn tại
    os.makedirs("playlists", exist_ok=True)
    
    # Lưu hàng đợi với tên được chỉ định
    playlist_file = f"playlists/{guild_id}_{playlist_name}.json"
    with open(playlist_file, "w", encoding="utf-8") as f:
        json.dump(music_queues[guild_id], f, ensure_ascii=False, indent=4)
    
    await interaction.response.send_message(
        f"✅ Đã lưu {len(music_queues[guild_id])} bài hát vào playlist **{playlist_name}**",
        ephemeral=False
    )

@bot.tree.command(name="loadqueue", description="Tải một playlist đã lưu trước đó")
async def loadqueue(interaction: discord.Interaction, playlist_name: str):
    await interaction.response.defer(ephemeral=False, thinking=True)
    guild_id = str(interaction.guild.id)
    
    # Kiểm tra playlist tồn tại
    playlist_file = f"playlists/{guild_id}_{playlist_name}.json"
    if not os.path.exists(playlist_file):
        await interaction.followup.send(f"Không tìm thấy playlist **{playlist_name}**")
        return
        
    # Tải playlist
    with open(playlist_file, "r", encoding="utf-8") as f:
        saved_queue = json.load(f)
    
    # Thêm vào hàng đợi hiện tại
    if guild_id not in music_queues:
        music_queues[guild_id] = []
    
    music_queues[guild_id].extend(saved_queue)
    
    # Bắt đầu phát nếu chưa phát
    voice_client = interaction.guild.voice_client
    if voice_client and voice_client.is_connected() and not voice_client.is_playing():
        await play_next_stream(guild_id, voice_client)
    
    await interaction.followup.send(
        f"✅ Đã tải {len(saved_queue)} bài hát từ playlist **{playlist_name}**"
    )

@bot.tree.command(name="playnext", description="Thêm nhạc vào vị trí tiếp theo trong hàng đợi")
async def playnext(interaction: discord.Interaction, query: str):
    # Ngay lập tức phản hồi để giữ tương tác còn sống
    await interaction.response.defer(ephemeral=False, thinking=True)
    
    # Kiểm tra người dùng có ở trong voice channel không
    if interaction.user.voice is None:
        await interaction.followup.send("Cậu cần ở trong voice channel để Shore phát nhạc.")
        return
        
    voice_channel = interaction.user.voice.channel
    guild_id = str(interaction.guild.id)
    
    # Kết nối tới voice channel nếu chưa kết nối
    if interaction.guild.voice_client is None:
        try:
            voice_client = await voice_channel.connect()
        except Exception as e:
            await interaction.followup.send(f"Không thể kết nối voice channel: {str(e)}")
            return
    else:
        voice_client = interaction.guild.voice_client
        
    # Kiểm tra channel có phải là cùng một channel không
    if voice_client.channel != voice_channel:
        await interaction.followup.send(f"Shore đang ở trong voice channel khác: {voice_client.channel.mention}")
        return
        
    # Khởi tạo hàng đợi cho guild này nếu chưa tồn tại
    if guild_id not in music_queues:
        music_queues[guild_id] = []
    
    # Kiểm tra xem input có phải là URL không
    if is_url(query):
        # Xử lý URL (tương tự như lệnh play nhưng thêm vào vị trí 0)
        try:
            # Xử lý URL tương tự như trong hàm play
            is_playlist = is_playlist_url(query)
            
            ydl_opts = get_ydl_opts(is_playlist, is_playlist)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(query, download=False)
                
                if 'entries' in info:
                    info = info['entries'][0]
                
                video_title = info.get('title', 'Unknown')
                video_url = query if not 'id' in info else f"https://www.youtube.com/watch?v={info['id']}"
                
                # Thêm vào đầu hàng đợi
                song_info = {
                    'title': video_title,
                    'url': video_url,
                    'requester': interaction.user.name
                }
                
                music_queues[guild_id].insert(0, song_info)
                await interaction.followup.send(f"🎵 Đã thêm **{video_title}** vào vị trí tiếp theo trong hàng đợi.", ephemeral=False)
                
                # Bắt đầu phát nếu chưa phát
                if not voice_client.is_playing():
                    await play_next_stream(guild_id, voice_client)
        except Exception as e:
            await interaction.followup.send(f"❌ Lỗi khi xử lý URL: {str(e)}", ephemeral=False)
    else:
        # Xử lý tìm kiếm
        try:
            now = time.time()
            
            # Kiểm tra cache
            if query in search_cache and now < cache_expiry.get(query, 0):
                # Sử dụng kết quả từ cache
                video_title = search_cache[query]["title"]
                video_url = search_cache[query]["url"]
            else:
                # Tìm kiếm bình thường
                ydl_opts = get_ydl_opts()  # Không cần truyền tham số is_playlist
                ydl_opts['default_search'] = 'ytsearch1'  # Thêm tùy chọn tìm kiếm
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # Thêm code xử lý tìm kiếm
                    info = ydl.extract_info(f"ytsearch:{query}", download=False)
                    if 'entries' in info and info['entries']:
                        video = info['entries'][0]
                        video_title = video.get('title', 'Unknown')
                        video_url = f"https://www.youtube.com/watch?v={video['id']}"
                        
                        # Lưu vào cache
                        search_cache[query] = {"title": video_title, "url": video_url}
                        cache_expiry[query] = now + CACHE_DURATION
                    else:
                        await interaction.followup.send(f"❌ Không tìm thấy kết quả nào cho '{query}'")
                        return
            
            # Thêm vào đầu hàng đợi
            song_info = {
                'title': video_title,
                'url': video_url,
                'requester': interaction.user.name
            }
            
            music_queues[guild_id].insert(0, song_info)
            
            await interaction.followup.send(f"🎵 Đã thêm **{video_title}** vào vị trí tiếp theo trong hàng đợi.", ephemeral=False)
            
            # Bắt đầu phát nếu chưa phát
            if not voice_client.is_playing():
                await play_next_stream(guild_id, voice_client)
                
        except Exception as e:
            await interaction.followup.send(f"❌ Lỗi khi tìm kiếm: {str(e)}", ephemeral=False)

@bot.tree.command(name="clearqueue", description="Xóa toàn bộ hàng đợi nhạc")
async def clearqueue(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    # Check if queue exists and has songs
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.response.send_message("Hàng đợi nhạc đã trống.", ephemeral=True)
        return
    
    # Create confirmation view
    class ClearQueueView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
            self.confirmed = False
            
        @discord.ui.button(label="Xác nhận xóa", style=discord.ButtonStyle.danger)
        async def confirm(self, confirmation_interaction: discord.Interaction, button: discord.ui.Button):
            self.confirmed = True
            self.stop()
            
            # Clear the queue
            queue_size = len(music_queues[guild_id])
            music_queues[guild_id] = []
            
            await confirmation_interaction.response.send_message(
                f"🗑️ Đã xóa {queue_size} bài hát khỏi hàng đợi.",
                ephemeral=False
            )
            
        @discord.ui.button(label="Hủy", style=discord.ButtonStyle.secondary)
        async def cancel(self, confirmation_interaction: discord.Interaction, button: discord.ui.Button):
            self.stop()
            await confirmation_interaction.response.send_message(
                "❌ Đã hủy xóa hàng đợi.",
                ephemeral=True
            )
    
    queue_size = len(music_queues[guild_id])
    view = ClearQueueView()
    
    await interaction.response.send_message(
        f"⚠️ Bạn có chắc chắn muốn xóa tất cả {queue_size} bài hát khỏi hàng đợi không?",
        view=view,
        ephemeral=True
    )
    
@bot.tree.command(name="nowplaying", description="Hiển thị thông tin bài hát đang phát")
async def nowplaying_alias(interaction: discord.Interaction):
    # Reuse the nowplaying function
    await nowplaying(interaction)    
    
@bot.tree.command(name="np", description="Hiển thị thông tin bài hát đang phát theo thời gian thực")
async def nowplaying(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    voice_client = interaction.guild.voice_client
    
    # Check if bot is in voice channel
    if voice_client is None or not voice_client.is_connected():
        await interaction.response.send_message("Shore không ở trong voice channel nào.", ephemeral=True)
        return
    
    # Check if a song is currently playing
    if guild_id not in currently_playing:
        await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)
        return
    
    # Tạo view
    view = NowPlayingView(guild_id)
    
    # Tạo và gửi embed ban đầu
    current = currently_playing[guild_id]
    embed = discord.Embed(
        title="🎵 Đang phát",
        description=f"**{current['title']}**",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Thêm thông tin cơ bản
    embed.add_field(name="Yêu cầu bởi", value=current['requester'], inline=True)
    
    queue_length = len(music_queues.get(guild_id, []))
    embed.add_field(name="Hàng đợi", value=f"{queue_length} bài tiếp theo", inline=True)
    
    # Thêm tiến độ ban đầu
    if 'start_time' in current:
        elapsed_seconds = int(time.time() - current['start_time'])
        minutes, seconds = divmod(elapsed_seconds, 60)
        elapsed_str = f"{minutes}:{seconds:02d}"
        
        # Thanh tiến độ ban đầu
        progress_bar = "▰▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱▱"
        
        embed.add_field(
            name="Tiến độ phát",
            value=f"`{elapsed_str}` {progress_bar} `-??:??`\nNhấn 'Tự động cập nhật' để hiển thị tiến độ theo thời gian thực",
            inline=False
        )
    
    embed.set_footer(text="Nhấn 'Cập nhật' để cập nhật thủ công, hoặc 'Tự động cập nhật' để cập nhật liên tục")
    
    # Gửi tin nhắn
    await interaction.response.send_message(embed=embed, view=view)
    
    # Lưu message để cập nhật sau
    response = await interaction.original_response()
    view.message = response

@bot.tree.command(name="remove", description="Xóa một bài hát cụ thể khỏi hàng đợi")
async def remove(interaction: discord.Interaction, position: int):
    guild_id = str(interaction.guild.id)
    
    # Check if queue exists and has songs
    if guild_id not in music_queues or not music_queues[guild_id]:
        await interaction.response.send_message("Hàng đợi nhạc đang trống.", ephemeral=True)
        return
    
    # Check if position is valid
    if position < 1 or position > len(music_queues[guild_id]):
        await interaction.response.send_message(
            f"Vị trí không hợp lệ. Hãy chọn số từ 1 đến {len(music_queues[guild_id])}.", 
            ephemeral=True
        )
        return
    
    # Remove the song at the specified position
    position = position - 1  # Convert to 0-based index
    removed_song = music_queues[guild_id].pop(position)
    
    # Create an embed for the response
    embed = discord.Embed(
        title="🗑️ Đã xóa bài hát",
        description=f"Đã xóa **{removed_song['title']}** khỏi hàng đợi.",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Add current queue length
    queue_length = len(music_queues.get(guild_id, []))
    embed.set_footer(text=f"Còn lại {queue_length} bài hát trong hàng đợi")
    
    # Send the response
    await interaction.response.send_message(embed=embed, ephemeral=False)

async def cleanup_temp_files():
    await bot.wait_until_ready()
    while not bot.is_closed():
        # Xóa các file tạm cũ
        for file in os.listdir():
            if file.startswith("welcome_") or file.startswith("speak_msg_"):
                if os.path.getctime(file) < time.time() - 3600:  # cũ hơn 1 giờ
                    try:
                        os.remove(file)
                    except:
                        pass
        await asyncio.sleep(3600)

def save_music_queues():
    """Lưu hàng đợi nhạc vào file JSON"""
    data = {
        "music_queues": music_queues,
        "currently_playing": currently_playing
    }
    with open("music_data.json", "w", encoding="utf-8") as f:
        # Chuyển đổi các key từ int sang str để JSON serializable
        json_data = {}
        for guild_id, queue in music_queues.items():
            json_data[str(guild_id)] = queue
        
        json.dump(json_data, f, ensure_ascii=False, indent=4)

def load_music_queues():
    """Tải hàng đợi nhạc từ file JSON"""
    global music_queues, currently_playing
    try:
        if os.path.exists("music_data.json"):
            with open("music_data.json", "r", encoding="utf-8") as f:
                data = json.load(f)
                music_queues = data
    except Exception as e:
        print(f"Lỗi khi tải music_queues: {e}")
        music_queues = {}

# Tạo class view mới với khả năng cập nhật
class NowPlayingView(discord.ui.View):
    def __init__(self, guild_id):
        super().__init__(timeout=300)  # View hết hạn sau 5 phút
        self.guild_id = guild_id
        self.auto_update = False
        self.message = None
        self.update_task = None
        
    async def on_timeout(self):
        # Dừng việc cập nhật khi view hết hạn
        if self.update_task:
            self.update_task.cancel()
        # Vô hiệu hóa các nút
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass
    
    @discord.ui.button(label="Cập nhật", emoji="🔄", style=discord.ButtonStyle.secondary)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.update_embed()
        await interaction.followup.send("Đã cập nhật thông tin bài hát", ephemeral=True)
    
    @discord.ui.button(label="Tự động cập nhật", style=discord.ButtonStyle.primary)
    async def toggle_auto_update(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.auto_update = not self.auto_update
        
        if self.auto_update:
            button.label = "Dừng cập nhật tự động"
            button.style = discord.ButtonStyle.danger
            await interaction.response.send_message("Đã bật tự động cập nhật tiến độ nhạc", ephemeral=True)
            
            # Bắt đầu task cập nhật
            if not self.update_task or self.update_task.done():
                self.update_task = asyncio.create_task(self.auto_update_loop())
        else:
            button.label = "Tự động cập nhật"
            button.style = discord.ButtonStyle.primary
            if self.update_task and not self.update_task.done():
                self.update_task.cancel()
            await interaction.response.send_message("Đã tắt tự động cập nhật tiến độ nhạc", ephemeral=True)
        
        await interaction.message.edit(view=self)
    
    @discord.ui.button(label="⏭️ Bỏ qua", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.guild_id
        voice_client = interaction.guild.voice_client
        
        if not voice_client or not voice_client.is_connected() or not voice_client.is_playing():
            await interaction.response.send_message("Không có bài hát nào đang phát.", ephemeral=True)
            return
        
        # Stop the current song
        voice_client.stop()
        await interaction.response.send_message("⏭️ Đã bỏ qua bài hát hiện tại", ephemeral=True)
    
    async def update_embed(self):
        if not self.message:
            return
            
        guild_id = self.guild_id
        current = currently_playing.get(guild_id)
        
        if not current:
            return
            
        # Tạo embed mới
        embed = discord.Embed(
            title="🎵 Đang phát",
            description=f"**{current['title']}**",
            color=0x3498db,
            timestamp=datetime.now()
        )
        
        # Thêm thông tin người yêu cầu
        embed.add_field(
            name="Yêu cầu bởi", 
            value=current['requester'],
            inline=True
        )
        
        # Thêm thông tin hàng đợi
        queue_length = len(music_queues.get(guild_id, []))
        embed.add_field(
            name="Hàng đợi",
            value=f"{queue_length} bài tiếp theo",
            inline=True
        )
        
        # Tính thời gian đã phát
        if 'start_time' in current:
            elapsed_seconds = int(time.time() - current['start_time'])
            minutes, seconds = divmod(elapsed_seconds, 60)
            elapsed_str = f"{minutes}:{seconds:02d}"
            
            # Ước tính thời lượng bài hát (thường là 3-4 phút)
            estimated_total = 240
            progress = min(1.0, elapsed_seconds / estimated_total)
            bar_length = 20
            filled_bars = int(bar_length * progress)
            progress_bar = "▰" * filled_bars + "▱" * (bar_length - filled_bars)
            
            # Ước tính thời gian còn lại
            remaining = max(0, estimated_total - elapsed_seconds)
            r_minutes, r_seconds = divmod(remaining, 60)
            remaining_str = f"{r_minutes}:{r_seconds:02d}"
            
            embed.add_field(
                name="Tiến độ phát",
                value=f"`{elapsed_str}` {progress_bar} `-{remaining_str}`",
                inline=False
            )
        
        embed.set_footer(text="Cập nhật lúc: " + datetime.now().strftime("%H:%M:%S"))
        
        # Cập nhật message
        try:
            await self.message.edit(embed=embed)
        except Exception as e:
            print(f"Error updating nowplaying embed: {e}")
            if self.update_task:
                self.update_task.cancel()
                self.auto_update = False
    
    async def auto_update_loop(self):
        try:
            while self.auto_update:
                await self.update_embed()
                await asyncio.sleep(3)  # Cập nhật 3 giây một lần
        except asyncio.CancelledError:
            # Task bị hủy khi tắt auto update
            pass
        except Exception as e:
            print(f"Error in auto update loop: {e}")

#---------------GAME-----------------------

@bot.tree.command(name="activity", description="Hiển thị danh sách hoạt động của các thành viên trong server")
async def activity(interaction: discord.Interaction):
    """Hiển thị danh sách hoạt động hiện tại của các thành viên"""
    # Phản hồi ngay để tránh timeout
    await interaction.response.defer(ephemeral=False, thinking=True)
    
    # Lấy thông tin server
    guild = interaction.guild
    
    # Dictionary lưu hoạt động và thành viên
    activities = {}  # {activity_name: {'emoji': emoji, 'members': [member_ids]}}
    
    # Theo dõi người trong voice channel 
    voice_channels = {}  # {channel_id: [member_ids]}
    
    # Đếm số người online
    online_count = 0
    
    # Khởi tạo field_count tại đây
    field_count = 0
    
    # Kiểm tra tất cả thành viên
    for member in guild.members:
        if member.bot:  # Bỏ qua bot
            continue
            
        # Đếm người online
        if member.status != discord.Status.offline:
            online_count += 1
            
        # Theo dõi người trong voice channel
        if member.voice and member.voice.channel:
            channel_id = member.voice.channel.id
            if channel_id not in voice_channels:
                voice_channels[channel_id] = []
            voice_channels[channel_id].append(member.id)
            
        # Kiểm tra hoạt động (game, streaming, v.v.)
        for activity in member.activities:
            # Bỏ qua hoạt động không có tên
            if not hasattr(activity, 'name') or not activity.name:
                continue
                
            activity_name = activity.name
            emoji = "🎮"  # Emoji mặc định
            
            # Check all activity types, not just playing
            if isinstance(activity, discord.Activity) or isinstance(activity, discord.Game) or activity.type == discord.ActivityType.playing:
                # Game detection code
                game_lower = activity_name.lower()
                if "valorant" in game_lower:
                    emoji = "🔫"
                elif "league of legends" in game_lower or "lol" in game_lower:
                    emoji = "🏆"
                elif "minecraft" in game_lower:
                    emoji = "⛏️"
                elif "honkai" in game_lower or "star rail" in game_lower or "hsr" in game_lower:
                    emoji = "🚂"
                elif "zenless" in game_lower or "zzz" in game_lower:
                    emoji = "⚡"
                elif "wuthering" in game_lower or "waves" in game_lower:
                    emoji = "🌊"
                elif "roblox" in game_lower:
                    emoji = "🟥"
                elif "overwatch" in game_lower:
                    emoji = "🛡️"
            elif activity.type == discord.ActivityType.streaming:
                activity_name = f"Streaming: {activity_name}"
                emoji = "📡"
            elif activity.type == discord.ActivityType.listening:
                if isinstance(activity, discord.Spotify):
                    activity_name = f"Spotify: {activity.artist} - {activity.title}"
                else:
                    activity_name = f"Listening: {activity_name}"
                emoji = "🎧"
            elif activity.type == discord.ActivityType.watching:
                activity_name = f"Watching: {activity_name}"
                emoji = "📺"
            elif activity.type == discord.ActivityType.competing:
                activity_name = f"Competing: {activity_name}"
                emoji = "🏆"
            elif activity.type == discord.ActivityType.custom:
                emoji = "📝"
            
            # Add activity to the dictionary regardless of type
            if activity_name not in activities:
                activities[activity_name] = {'emoji': emoji, 'members': []}
            if member.id not in activities[activity_name]['members']:
                activities[activity_name]['members'].append(member.id)
    
    # Tạo embed
    embed = discord.Embed(
        title="🎮 Hoạt động của thành viên",
        description=f"Thành viên online: **{online_count}**",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Thêm các field hoạt động
    if activities:
        # Sắp xếp hoạt động theo số lượng thành viên (giảm dần)
        sorted_activities = sorted(
            [(name, data) for name, data in activities.items()],
            key=lambda x: len(x[1]['members']), 
            reverse=True
        )
        
        # Giới hạn tối đa 20 hoạt động để tránh vượt quá giới hạn field của embed
        field_count = 0
        for activity_name, data in sorted_activities[:20]:
            emoji = data['emoji']
            members = data['members']
            
            # Bỏ qua nếu không có thành viên
            if not members:
                continue
                
            # Tạo danh sách mentions, giới hạn tối đa 20 thành viên mỗi hoạt động
            if len(members) > 20:
                members_text = ", ".join(f"<@{member_id}>" for member_id in members[:20])
                members_text += f"\n*...và {len(members) - 20} người khác*"
            else:
                members_text = ", ".join(f"<@{member_id}>" for member_id in members)
            
            # Cắt ngắn nếu quá dài
            if len(members_text) > 1024:
                members_text = members_text[:1000] + "...\n*quá nhiều để hiển thị*"
            
            embed.add_field(
                name=f"{emoji} {activity_name} ({len(members)})",
                value=members_text,
                inline=False
            )
            
            field_count += 1
            if field_count >= 20:  # Để dành chỗ cho field voice channel
                break
        
        # Nếu phải cắt bớt danh sách hoạt động
        remaining_count = len(sorted_activities) - field_count
        if remaining_count > 0:
            embed.add_field(
                name="📋 Hoạt động khác",
                value=f"*{remaining_count} hoạt động khác không được hiển thị do giới hạn*",
                inline=False
            )
    
    # Thêm field voice channel
    if voice_channels and field_count < 24:  # Để dành chỗ cho ít nhất một field voice
        voice_field_value = ""
        
        # Sắp xếp kênh theo số lượng thành viên
        sorted_channels = sorted(
            [(channel_id, members) for channel_id, members in voice_channels.items()],
            key=lambda x: len(x[1]), 
            reverse=True
        )
        
        for channel_id, members in sorted_channels:
            channel = guild.get_channel(channel_id)
            if channel and members:
                # Giới hạn tối đa 20 thành viên mỗi kênh
                if len(members) > 20:
                    members_text = ", ".join(f"<@{member_id}>" for member_id in members[:20])
                    members_text += f" *...và {len(members) - 20} người khác*"
                else:
                    members_text = ", ".join(f"<@{member_id}>" for member_id in members)
                
                channel_text = f"**{channel.name}** ({len(members)}): {members_text}\n\n"
                
                # Kiểm tra nếu thêm sẽ vượt quá giới hạn
                if len(voice_field_value) + len(channel_text) > 1024:
                    voice_field_value += "*Các kênh voice khác không hiển thị do giới hạn*"
                    break
                    
                voice_field_value += channel_text
        
        if voice_field_value:
            embed.add_field(
                name="🔊 Voice Channels",
                value=voice_field_value,
                inline=False
            )
    
    # Nếu không có hoạt động hoặc voice channels
    if not activities and not voice_channels:
        embed.add_field(
            name="Không có hoạt động",
            value="Không có thành viên nào đang thực hiện hoạt động.",
            inline=False
        )
    
    # Gửi embed
    await interaction.followup.send(embed=embed)

def create_game_embed(game_name: str, players: list):
    """Tạo embed với màu sắc và hình ảnh tương ứng với game"""
    # Chuẩn hóa tên game để so sánh
    game_lower = game_name.lower()
    
    # Embed mặc định
    embed = discord.Embed(
        title=f"🎮 {game_name}",
        description="Danh sách người chơi:",
        color=0x3498db,
        timestamp=datetime.now()
    )
    
    # Định dạng đặc biệt cho các game phổ biến
    if "liên minh" in game_lower or "lol" in game_lower or "league of legends" in game_lower:
        embed.title = f"🏆 Liên Minh Huyền Thoại"
        embed.color = discord.Color.blue()
        embed.set_thumbnail(url="https://static.wikia.nocookie.net/leagueoflegends/images/0/06/League_of_Legends_icon.png")
    elif "valorant" in game_lower:
        embed.title = f"🔫 Valorant"
        embed.color = discord.Color.red()
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/f/fc/Valorant_logo_-_pink_color_version.svg/1200px-Valorant_logo_-_pink_color_version.svg.png")
    elif "minecraft" in game_lower or "mine craft" in game_lower:
        embed.title = f"⛏️ Minecraft"
        embed.color = discord.Color.green()
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/6/61/Minecraft_desktop_logo.svg/1200px-Minecraft_desktop_logo.svg.png")
    elif "honkai" in game_lower or "star rail" in game_lower or "hsr" in game_lower:
        embed.title = f"🚂 Honkai: Star Rail"
        embed.color = discord.Color.from_rgb(101, 137, 226)
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/en/b/b1/Honkai_Star_Rail.jpg")
    elif "zenless" in game_lower or "zzz" in game_lower:
        embed.title = f"⚡ Zenless Zone Zero"
        embed.color = discord.Color.from_rgb(255, 136, 0)
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/en/9/95/Zenless_Zone_Zero_logo.jpg")
    elif "wuthering" in game_lower or "waves" in game_lower:
        embed.title = f"🌊 Wuthering Waves"
        embed.color = discord.Color.from_rgb(0, 191, 255)
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/en/1/15/Wuthering_Waves_logo.png")
    elif "roblox" in game_lower:
        embed.title = f"🟥 Roblox"
        embed.color = discord.Color.from_rgb(226, 35, 26)
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Roblox_player_icon_black.svg/1200px-Roblox_player_icon_black.svg.png")
    elif "overwatch" in game_lower:
        embed.title = f"🛡️ Overwatch 2"
        embed.color = discord.Color.from_rgb(243, 144, 29)
        embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/thumb/7/72/Overwatch_2_logo.svg/1200px-Overwatch_2_logo.svg.png")
    
    # Thêm danh sách người chơi
    if players:
        players_text = "\n".join(f"<@{player_id}>" for player_id in players)
        embed.add_field(name=f"Người chơi ({len(players)})", value=players_text, inline=False)
    else:
        embed.add_field(name="Người chơi (0)", value="*Chưa có ai tham gia*", inline=False)
    
    # Thêm footer
    embed.set_footer(text="Nhấn 'Tham gia' để vào phòng chơi game này")
    
    return embed

class GameSessionView(discord.ui.View):
    def __init__(self, game_name: str, creator_id: int):
        super().__init__(timeout=None)  # View không timeout
        self.game_name = game_name
        self.creator_id = creator_id
        self.players = [creator_id]  # Người tạo tự động tham gia
        
    @discord.ui.button(label="Tham gia", style=discord.ButtonStyle.primary, custom_id="game:join")
    async def join_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        
        if user_id in self.players:
            await interaction.response.send_message("Bạn đã tham gia phòng game này rồi!", ephemeral=True)
            return
        
        self.players.append(user_id)
        
        # Cập nhật embed
        embed = create_game_embed(self.game_name, self.players)
        await interaction.message.edit(embed=embed)
        
        await interaction.response.send_message(f"Bạn đã tham gia phòng chơi {self.game_name}!", ephemeral=True)

    @discord.ui.button(label="Rời khỏi", style=discord.ButtonStyle.secondary, custom_id="game:leave")
    async def leave_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        
        if user_id not in self.players:
            await interaction.response.send_message("Bạn chưa tham gia phòng game này!", ephemeral=True)
            return
        
        self.players.remove(user_id)
        
        # Cập nhật embed
        embed = create_game_embed(self.game_name, self.players)
        await interaction.message.edit(embed=embed)
        
        await interaction.response.send_message(f"Bạn đã rời khỏi phòng chơi {self.game_name}!", ephemeral=True)

    @discord.ui.button(label="Kết thúc", style=discord.ButtonStyle.danger, custom_id="game:end")
    async def end_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id = interaction.user.id
        
        # Chỉ người tạo hoặc admin mới có thể kết thúc
        if user_id != self.creator_id and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Chỉ người tạo phòng hoặc quản trị viên mới có thể kết thúc phòng game!", ephemeral=True)
            return
        
        # Vô hiệu hóa tất cả nút
        for child in self.children:
            child.disabled = True
        
        # Tạo embed kết thúc
        embed = discord.Embed(
            title=f"🎮 {self.game_name} - Đã kết thúc",
            description="Phòng game này đã kết thúc.",
            color=discord.Color.darker_gray(),
            timestamp=datetime.now()
        )
        
        embed.add_field(name=f"Người chơi đã tham gia ({len(self.players)})", 
                       value="\n".join(f"<@{player_id}>" for player_id in self.players) if self.players else "*Không có ai tham gia*", 
                       inline=False)
        
        await interaction.message.edit(embed=embed, view=self)
        await interaction.response.send_message("Phòng game đã kết thúc!", ephemeral=True)

@bot.tree.command(name="game", description="Tạo phòng chơi game và thông báo ai đang chơi cùng")
async def game(interaction: discord.Interaction, game_name: str):
    # Phản hồi ngay để tránh timeout
    await interaction.response.defer(ephemeral=False)
    
    # Tạo view cho phiên game
    view = GameSessionView(game_name, interaction.user.id)
    
    # Tạo embed ban đầu với người tạo là người chơi đầu tiên
    embed = create_game_embed(game_name, [interaction.user.id])
    
    # Gửi tin nhắn
    await interaction.followup.send(embed=embed, view=view)
    
    # Đăng ký view với bot để nó hoạt động
    bot.add_view(view)

# Thêm global dictionaries
user_xp = {}  # {guild_id: {user_id: xp}}
level_roles = {}  # {guild_id: {level: role_id}}

@bot.tree.command(name="level", description="Xem level của bạn hoặc người khác")
async def level(interaction: discord.Interaction, member: discord.Member = None):
    guild_id = str(interaction.guild.id)
    
    if member is None:
        member = interaction.user
    
    user_id = str(member.id)
    
    # Khởi tạo nếu chưa có
    if guild_id not in user_xp:
        user_xp[guild_id] = {}
    
    # Lấy XP hiện tại
    xp = user_xp[guild_id].get(user_id, 0)
    
    # Tính level (ví dụ: mỗi level cần 100*level XP)
    level = 0
    remaining_xp = xp
    while remaining_xp >= 100 * (level + 1):
        remaining_xp -= 100 * (level + 1)
        level += 1
    
    next_level_xp = 100 * (level + 1)
    
    # Tạo progress bar
    progress = remaining_xp / next_level_xp
    bar_length = 20
    filled_bars = int(bar_length * progress)
    progress_bar = "█" * filled_bars + "░" * (bar_length - filled_bars)
    
    embed = discord.Embed(
        title=f"Level của {member.display_name}",
        description=f"**Level {level}** ({xp} XP tổng cộng)",
        color=member.color
    )
    
    embed.add_field(
        name=f"Tiến độ đến Level {level+1}", 
        value=f"{progress_bar} {remaining_xp}/{next_level_xp} XP", 
        inline=False
    )
    
    # Thêm avatar
    embed.set_thumbnail(url=member.display_avatar.url)
    
    await interaction.response.send_message(embed=embed)
    
# Thêm global dictionaries
birthdays = {}  # {guild_id: {user_id: "MM-DD"}}

@bot.tree.command(name="setbirthday", description="Đặt ngày sinh nhật của bạn")
async def setbirthday(interaction: discord.Interaction, date: str):
    """Đặt ngày sinh nhật (định dạng MM-DD)"""
    
    # Kiểm tra định dạng
    try:
        datetime.strptime(date, "%m-%d")
    except ValueError:
        await interaction.response.send_message("Định dạng không hợp lệ. Hãy sử dụng MM-DD (vd: 05-25)", ephemeral=True)
        return
    
    guild_id = str(interaction.guild.id)
    user_id = str(interaction.user.id)
    
    # Lưu sinh nhật
    if guild_id not in birthdays:
        birthdays[guild_id] = {}
    
    birthdays[guild_id][user_id] = date
    
    # Lưu vào file
    with open("birthdays.json", "w") as f:
        json.dump(birthdays, f)
    
    await interaction.response.send_message(f"✅ Đã đặt sinh nhật của bạn vào ngày {date}!", ephemeral=False)

# Thêm vào on_ready
async def check_birthdays():
    await bot.wait_until_ready()
    while not bot.is_closed():
        today = datetime.now().strftime("%m-%d")
        
        for guild_id, guild_birthdays in birthdays.items():
            for user_id, bday in guild_birthdays.items():
                if bday == today:
                    # Tìm channel để gửi thông báo
                    shore_chan = get_shore_channel(guild_id)
                    if shore_chan:
                        await shore_chan.send(f"🎂 Hôm nay là sinh nhật của <@{user_id}>! Chúc mừng sinh nhật!")
        
        # Kiểm tra mỗi ngày vào lúc 7 giờ sáng
        now = datetime.now()
        tomorrow = datetime.combine(now.date() + timedelta(days=1), datetime.min.time()) + timedelta(hours=7)
        await asyncio.sleep((tomorrow - now).total_seconds())

@bot.tree.command(name="translate", description="Dịch văn bản sang ngôn ngữ khác")
async def translate(interaction: discord.Interaction, text: str, target_lang: str = "vi"):
    """Dịch văn bản sang ngôn ngữ khác (mặc định: tiếng Việt)"""
    await interaction.response.defer(ephemeral=False)
    
    try:
        # Phát hiện ngôn ngữ gốc
        detected_lang = GoogleTranslator().detect(text)
        
        # Dịch sang ngôn ngữ đích
        translator = GoogleTranslator(source='auto', target=target_lang)
        result = translator.translate(text)
        
        # Tạo embed kết quả
        embed = discord.Embed(
            title="🌐 Dịch thuật",
            color=0x3498db,
            timestamp=datetime.now()
        )
        
        embed.add_field(name=f"Văn bản gốc ({detected_lang})", value=text, inline=False)
        embed.add_field(name=f"Dịch sang {target_lang}", value=result, inline=False)
        
        # Thêm footer với các ngôn ngữ phổ biến
        embed.set_footer(text="Các ngôn ngữ phổ biến: en (Anh), vi (Việt), ja (Nhật), ko (Hàn), zh-cn (Trung), fr (Pháp), de (Đức)")
        
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Lỗi khi dịch thuật: {str(e)}")

async def check_rate_limit(command_name, user_id, cooldown=3.0):
    """Kiểm tra giới hạn tần suất sử dụng lệnh"""
    now = time.time()
    if command_name not in rate_limits:
        rate_limits[command_name] = {}
    
    # Kiểm tra thời gian sử dụng gần nhất
    if user_id in rate_limits[command_name]:
        last_used = rate_limits[command_name][user_id]
        if now - last_used < cooldown:
            return False
    
    # Cập nhật thời gian sử dụng
    rate_limits[command_name][user_id] = now
    return True

@bot.event
async def on_ready():
    await bot.wait_until_ready()
    load_music_queues()
    for plan_id, plan_data in plans.items():
        view = PlanView(plan_id=plan_id)
        if "voters" in plan_data:
            view.voters = set(plan_data["voters"])
        bot.add_view(view)
        if "event_time" in plan_data and "channel_id" in plan_data:
            try:
                event_dt = datetime.fromisoformat(plan_data["event_time"])
                if event_dt > datetime.now():
                    asyncio.create_task(schedule_reminder(plan_data["channel_id"], plan_id, event_dt))
            except (ValueError, TypeError):
                print(f"Invalid date format for plan {plan_id}")
                
    # Register persistent views for Valorant tournaments
    for guild_id, data in valorant_data.items():
        # If there's an active tournament with score tracking
        if "mode" in data and "score" in data:
            score_view = ScoreView(guild_id, data["mode"])
            bot.add_view(score_view)
            
    # Thêm các task vào đây
    bot.loop.create_task(daily_stats_task())
    bot.loop.create_task(weekly_stats_task())
    bot.loop.create_task(check_voice_channels())
    bot.loop.create_task(check_birthdays())  # Thêm task check_birthdays ở đây
    load_stream_cache()
    bot.add_view(MusicControlView())
    
    await bot.tree.sync()
    print(f"{bot.user} has connected to Discord!")

@bot.event
async def on_disconnect():
    save_music_queues()
    save_stream_cache()


# ---------------- MAIN ----------------
try:
    bot.run(BOT_TOKEN)
except Exception as e:
    print(f"Lỗi khi chạy bot: {e}")