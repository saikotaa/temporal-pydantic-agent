"use client";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useEffect } from "react";
import { Chat } from "@/components/chat";

function Page() {
  const params = useSearchParams();
  const router = useRouter();
  const conversationId = params.get("c");
  useEffect(() => {
    if (!conversationId) router.replace(`/?c=web-${crypto.randomUUID().slice(0, 8)}`);
  }, [conversationId, router]);
  if (!conversationId) return null;
  return <Chat conversationId={conversationId} />;
}

export default function Home() {
  return (
    <Suspense>
      <Page />
    </Suspense>
  );
}
