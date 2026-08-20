from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods

from .gamma_blast_engine import GammaBotError, get_gamma_bot


_gamma_bot = get_gamma_bot()


@require_http_methods(["GET", "POST"])
def start_gamma_bot(request):
	try:
		started = _gamma_bot.start()
	except GammaBotError as exc:
		return JsonResponse({"started": False, "error": str(exc), "status": _gamma_bot.status()}, status=503)
	return JsonResponse({"started": started, "status": _gamma_bot.status()})


@require_http_methods(["GET", "POST"])
def stop_gamma_bot(request):
	return JsonResponse({"stopped": _gamma_bot.stop(), "status": _gamma_bot.status()})


@require_GET
def gamma_bot_status(request):
	return JsonResponse(_gamma_bot.status())
