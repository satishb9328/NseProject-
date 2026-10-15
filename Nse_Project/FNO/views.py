import datetime
import sqlite3
import subprocess
import sys
from pathlib import Path

from django.http import JsonResponse
from django.shortcuts import render

from .fno_swing_engine import (
	DaySignals,
	SwingDB,
	SwingEngine,
	calculate_net_volume,
	generate_swing_signal,
	get_tomorrow_matrix,
	load_latest_volume_csv,
)


FNO_DATABASE_PATH = r"D:\FNO DATA\Participant data\Database\fno_swing.db"


def run_fno_update(request):
	"""Run the F&O data refresh from the FNO app directory."""
	if request.method != "POST":
		return JsonResponse({"status": "error", "message": "POST required."}, status=405)

	try:
		result = subprocess.run(
			[sys.executable, "fno_swing_engine.py", "update"],
			cwd=Path(__file__).resolve().parent,
			capture_output=True,
			text=True,
			check=False,
			timeout=600,
		)
	except subprocess.TimeoutExpired as error:
		stderr = error.stderr.decode(errors="replace") if isinstance(error.stderr, bytes) else error.stderr or ""
		return JsonResponse(
			{"status": "error", "message": "FNO update timed out.", "stderr": stderr},
			status=504,
		)
	except (OSError, subprocess.SubprocessError) as error:
		return JsonResponse(
			{"status": "error", "message": "FNO update could not run.", "stderr": str(error)},
			status=500,
		)

	if result.returncode != 0:
		return JsonResponse(
			{
				"status": "error",
				"message": "FNO update failed.",
				"stderr": result.stderr,
				"output": result.stdout or result.stderr,
			},
			status=500,
		)

	return JsonResponse(
		{"status": "success", "message": "FNO data updated.", "output": result.stdout},
		status=200,
	)


def _load_daily_report():
	"""Return the latest report values in a template-friendly structure."""
	SwingEngine()
	db = SwingDB(FNO_DATABASE_PATH)
	signals = db.latest_signals(1)
	fii_rows = db.latest_participant("FII", 1)
	pro_rows = db.latest_participant("PRO", 1)
	db.close()
	if not signals:
		return None

	latest = signals[0]
	fii_row = fii_rows[0] if fii_rows else {}
	pro_row = pro_rows[0] if pro_rows else {}
	if pro_row.get("date") != latest["date"]:
		pro_row = {}
	pro_total_oi = (pro_row.get("nopo") or 0) + (pro_row.get("npf") or 0)
	day = DaySignals(
		date=datetime.date.fromisoformat(latest["date"]),
		fii_npf=latest["fii_npf"],
		pro_nopo=latest["pro_nopo"],
		dii_combined=latest["dii_combined"],
		client_nfno=latest["client_nfno"],
		fii_short_pct=latest["fii_short_pct"],
		smart_money_score=latest["smart_money_score"],
		retail_trap=bool(latest["retail_trap"]),
		fii_pro_aligned=bool(latest["fii_pro_aligned"]),
		alignment_direction=latest["alignment_direction"],
		fii_client_diverged=bool(latest["fii_client_diverged"]),
		institutional_strength=latest["institutional_strength"],
		ai_sentiment=latest["ai_sentiment"],
		ai_confidence=latest["ai_confidence"],
	)
	signal = generate_swing_signal(day, fii_row)
	volume_data, volume_date = load_latest_volume_csv()
	volume_net = calculate_net_volume(volume_data) if volume_data else {}
	fii_net_vol = volume_net.get("FII", 0)
	pro_net_vol = volume_net.get("PRO", 0)
	if volume_data:
		action, bias, advice = get_tomorrow_matrix(
			day.fii_npf, fii_net_vol, pro_total_oi, pro_net_vol
		)
	else:
		action, bias, advice = (
			"N/A", "VOLUME DATA MISSING",
			"Download today's volume CSV to predict tomorrow.",
		)

	participants = {
		"FII": {"net_oi": day.fii_npf, "net_vol": volume_net.get("FII", 0)},
		"PRO": {"net_oi": pro_total_oi, "net_vol": pro_net_vol},
		"DII": {"net_oi": day.dii_combined, "net_vol": volume_net.get("DII", 0)},
		"CLIENT": {"net_oi": day.client_nfno, "net_vol": volume_net.get("CLIENT", 0)},
	}
	return {
		"oi_date": day.date.strftime("%d %b %Y (%A)"),
		"volume_date": volume_date or "Missing",
		"participants": participants,
		"action": action,
		"bias": bias,
		"advice": advice,
		"entry": signal.entry_note,
		"stop": signal.stop_note,
		"trade_bias": signal.bias,
		"trade_strength": signal.strength,
		"warnings_list": signal.warnings,
		"score": day.smart_money_score,
		"confidence": day.ai_confidence,
		"sentiment": day.ai_sentiment,
	}


def fno_performance_view(request):
	history = []
	daily_report = None
	score_distribution = {
		"labels": ["<= -4", "-3 to -2", "-1 to 1", "2 to 3", ">= 4"],
		"values": [0, 0, 0, 0, 0],
	}
	error_message = None

	if not Path(FNO_DATABASE_PATH).exists():
		error_message = "Performance database is not available yet."
	else:
		try:
			daily_report = _load_daily_report()
			with sqlite3.connect(FNO_DATABASE_PATH) as connection:
				connection.row_factory = sqlite3.Row
				recent_rows = connection.execute(
					"""
					SELECT date, fii_npf, pro_nopo, dii_combined, client_nfno,
						   fii_short_pct, smart_money_score, alignment_direction,
						   ai_sentiment, ai_confidence
					FROM cross_signals
					ORDER BY date DESC
					LIMIT 10
					"""
				).fetchall()
				distribution_rows = connection.execute(
					"SELECT smart_money_score FROM cross_signals"
				).fetchall()

			history = [dict(row) for row in reversed(recent_rows)]
			for row in distribution_rows:
				score = row["smart_money_score"]
				if score <= -4:
					score_distribution["values"][0] += 1
				elif score <= -2:
					score_distribution["values"][1] += 1
				elif score <= 1:
					score_distribution["values"][2] += 1
				elif score <= 3:
					score_distribution["values"][3] += 1
				else:
					score_distribution["values"][4] += 1
		except (sqlite3.Error, OSError) as exc:
			error_message = f"Unable to load performance data: {exc}"

	context = {
		"daily_report": daily_report,
		"history": history,
		"score_distribution": score_distribution,
		"error_message": error_message,
	}
	return render(request, "performance.html", context)
