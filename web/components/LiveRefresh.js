"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function LiveRefresh() {
  const router = useRouter();

  useEffect(() => {
    const events = new EventSource("/api/events");
    const refresh = () => router.refresh();
    events.addEventListener("update", refresh);
    return () => events.close();
  }, [router]);

  return null;
}
